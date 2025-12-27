from confluent_kafka import Producer
from uuid import uuid4


config = {
    'bootstrap.servers': 'localhost:9092',  # Kafka server address
    'group.id': 'my-first-group',          # Consumer group name
    'auto.offset.reset': 'earliest'         # Start reading from beginning
}

producer = Producer(config)

file = open('cleaned_dataset.csv', 'r')
line = file.readline()
while line:
    line = file.readline()
    producer.produce(topic="Messages", key=str(uuid4()), value=line.encode('utf-8'))
    producer.flush()

file.close()