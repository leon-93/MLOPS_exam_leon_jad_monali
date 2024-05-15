import requests
import json

payload = {'key1': 'value1', 'key2': 'value2'}

r = requests.get('https://httpbin.org/get', params=payload)
data = r.json()

with open('./data/api/results.json', 'w') as f:
    json.dump(data, f)