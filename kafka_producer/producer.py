import requests
from kafka import KafkaProducer


producer = KafkaProducer(bootstrap_servers=['training.io:9092'])

r = requests.get("https://stream.meetup.com/2/rsvps", stream=True)

for line in r.iter_lines():
	producer.send('meetup_topic', line)

