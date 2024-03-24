import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
from kafka import KafkaConsumer

from utils import classify_entities
from db_client import DBClient

class Classify(beam.DoFn):
    def process(self, element):
        classified_entities = classify_entities(element, entities)
        for entity in classified_entities:
            yield entity

def run_pipeline():
    # Set Kafka consumer configurations
    kafka_bootstrap_servers = 'localhost:9092'
    kafka_topic = 'events'

    # Define Apache Beam pipeline options
    options = PipelineOptions()

    # Create a pipeline
    with beam.Pipeline(options=options) as pipeline:
        # Read data from Kafka
        kafka_consumer = KafkaConsumer(bootstrap_servers=kafka_bootstrap_servers,
                                       auto_offset_reset='earliest',
                                       enable_auto_commit=True,
                                       group_id='beam-group',
                                       value_deserializer=lambda x: x.decode('utf-8'))
        kafka_consumer.subscribe([kafka_topic])

        # Apply Beam transform to classify data
        classified_data = (pipeline
                           | 'Read from Kafka' >> beam.Create(kafka_consumer)
                           | 'Classify Data' >> beam.ParDo(Classify()))

        # Write data to PostgreSQL database
        classified_data | 'Write to Database' >> beam.ParDo(WriteToDatabase(db_connection_string))

class WriteToDatabase(beam.DoFn):
    def __init__(self, db_client):
        self.client = db_client

    def process(self, element):
        try:
            # Write data to the database
            self.client.write(**element)
        except Exception as e:
            print("Error:", e)

if __name__ == '__main__':
    with open('./entities.txt', 'rb') as f:
        entities = f.readlines()

    entities = [entity.strip() for entity in entities]

    # Set PostgreSQL connection configurations
    db_connection_string = "dbname='seedtag' user='said' host='localhost' password='seedtag' port='5432'"
    db_client = DBClient(connection_string=db_connection_string)
    db_client.init_table()

    run_pipeline()
