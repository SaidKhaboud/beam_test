import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
from confluent_kafka import Consumer

from utils import classify_entities
from db_client import DBClient

class Classify(beam.DoFn):
    def process(self, element):
        classified_entities = classify_entities(element, entities)
        for entity in classified_entities:
            print("Entity: ",entity)
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
                print("message is None")
                pass
            elif msg.error():
                print("Consumer error: {}".format(msg.error()))
                pass
            else:
                yield msg.value().decode('utf-8')
    
    def teardown(self):
        self.consumer.close()

def run_pipeline():
    # Define Apache Beam pipeline options
    options = PipelineOptions()

    # Set Kafka consumer configurations
    kafka_bootstrap_servers = 'localhost:9092'
    kafka_topic = 'events'

    # Create a pipeline
    with beam.Pipeline(options=options) as pipeline:
        # Read data from Kafka
        kafka_consumer = Consumer({
                'bootstrap.servers': 'kafka',
                'group.id': 'mygroup',
                'auto.offset.reset': 'earliest'
            })

        kafka_consumer.subscribe(['events'])

        # Apply Beam transform to classify data
        classified_data = (pipeline
                           | 'Dummy transform' >> beam.Create([None])
                           | 'Kafka Consumer' >> beam.ParDo(ReadFromKafka())
                           | 'Classify Data' >> beam.ParDo(Classify()))

        # Write data to PostgreSQL database
        # classified_data | 'Write to Database' >> beam.ParDo(WriteToDatabase(db_connection_string))

class WriteToDatabase(beam.DoFn):
    def __init__(self, db_client):
        self.client = db_client

    def process(self, element):
        try:
            # Write data to the database
            print(element)
            self.client.write(**element)
        except Exception as e:
            print("Error:", e)

if __name__ == '__main__':
    with open('./entities.txt', 'rb') as f:
        entities = f.readlines()

    entities = [str(entity.strip()) for entity in entities]

    # Set PostgreSQL connection configurations
    db_connection_string = "postgresql+psycopg2://said:seedtag@postgres:5433/seedtag"
    # db_client = DBClient(connection_string=db_connection_string)
    # db_client.init_table()
    print(len(entities))
    print("pipeline initiated")
    print("*"*10)
    run_pipeline()
