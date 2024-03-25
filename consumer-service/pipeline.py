import logging
import time

import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
from confluent_kafka import Consumer
from database import Raw_Entities, SessionLocal as db_client
from utils import classify_entities

logging.basicConfig(level=logging.INFO)

class Classify(beam.DoFn):
    def process(self, element):
        classified_entities = classify_entities(element, entities)
        for entity in classified_entities:
            logging.info("Entity: %s", entity)
            yield entity

class ReadFromKafka(beam.DoFn):
        
    def setup(self):
        self.consumer = Consumer({
                'bootstrap.servers': 'kafka',
                'group.id': 'mygroup',
                'auto.offset.reset': 'earliest'
            })
        self.consumer.subscribe(['events'])

    def process(self, element):
        while True:
            msg = self.consumer.poll(1.0)
            if msg is None:
                logging.info("message is None")
                pass
            elif msg.error():
                logging.info("Consumer error: {}".format(msg.error()))
                pass
            else:
                message = msg.value().decode('utf-8')
                yield message
            # add sleep to slow down
            # time.sleep(1)
    
    def teardown(self):
        self.consumer.close()

def run_pipeline():
    # Define Apache Beam pipeline options
    options = PipelineOptions()

    # Create a pipeline
    with beam.Pipeline(options=options) as pipeline:
        # Read data from Kafka and classify data
        classified_data = (pipeline
                           | 'Dummy transform' >> beam.Create([None])
                           | 'Kafka Consumer' >> beam.ParDo(ReadFromKafka())
                           | 'Classify Data' >> beam.ParDo(Classify()))

        # Write data to PostgreSQL database
        classified_data | 'Write to Database' >> beam.ParDo(WriteToDatabase())

class WriteToDatabase(beam.DoFn):
    def process(self, element):
        try:
            db_element = Raw_Entities(**element)
            with db_client() as db:
                db.add(db_element)
                db.commit()

        except Exception as e:
            logging.info("Error:", e)

if __name__ == '__main__':
    with open('./entities.txt', 'r') as f:
        entities = f.readlines()

    entities = [entity.strip() for entity in entities]
    
    logging.info("pipeline initiated")

    run_pipeline()
