import json
import requests
import re
import datetime

def classify_entities(element, entities):
    entry = json.loads(element).values()
    data = {"text": entry["text"]}
    label = requests.post('http://classification-service:5000/predict', json=data).json()["label"]
    pattern = r'\b(?:' + '|'.join(entities) + r')\b'

    # Find all matches in the text
    matches = re.findall(pattern, entry["text"])
    return [
        {"entity": match, 
         "label": label, 
         "readers": entry["readers"], 
         "timestamp": datetime.datetime.fromtimestamp(entry["timestamp"])}
         for match in matches
    ]

    