import requests
from kafka import KafkaProducer


producer = KafkaProducer(bootstrap_servers=['bigdataanalytics-worker-0.novalocal:6667'])

r = requests.get("https://stream.meetup.com/2/rsvps", stream=True)

for line in r.iter_lines():
	producer.send('meetup_topic', line)

