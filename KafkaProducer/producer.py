from confluent_kafka import Producer
from uuid import uuid4

config = {
    'bootstrap.servers': 'localhost:9092',  # Kafka server address
    'group.id': 'my-first-group',          # Consumer group name
    'auto.offset.reset': 'earliest'         # Start reading from beginning
}

producer = Producer(config)

while True:
    producer.produce(topic="Messages", key=str(uuid4()), value=str(uuid4()).encode('utf-8'))

    producer.flush()