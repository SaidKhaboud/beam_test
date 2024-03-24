import asyncio
import json
import requests
from confluent_kafka import Consumer

c = Consumer({
    'bootstrap.servers': 'kafka',
    'group.id': 'mygroup',
    'auto.offset.reset': 'earliest'
})

c.subscribe(['events'])

def get_matches(text, entities):
    matches = []
    for entity in entities:
        if entity in text:
            matches.append(entity)
    return matches

def consume_and_classify():
    with open('./entities.txt', 'rb') as f:
        entities = f.readlines()

    entities = [entity.strip() for entity in entities]
    entities = {entity: 0 for entity in entities}

    while True:
        msg = c.poll(1.0)

        if msg is None:
            continue
        if msg.error():
            print("Consumer error: {}".format(msg.error()))
            continue
        
        (id, time, readers, text) = json.loads(msg.value().decode('utf-8')).values()

        data = {"text": text}
        label = requests.post('http://classification-service:5000/predict', json=data).json()["label"]

        matches = get_matches(text, entities)

        for match in matches:
            entities[match] += readers

        print(f"ID: {id}, Time: {time}, Readers: {readers}, Text: {text[:10]}, Label: {label}")
        

if __name__ == "_main_":
     asyncio.run(consume_and_classify())
