import sys
from confluent_kafka import Consumer
config = {
    'bootstrap.servers': 'localhost:9092',  # Kafka server address
    'group.id': 'my-first-group',          # Consumer group name
    'auto.offset.reset': 'earliest'         # Start reading from beginning
}

consumer = Consumer(config)

consumer.subscribe(["Messages"])

while True:

    msg = consumer.poll(1.0)

    if msg is not None:
        print(f"Reseived message: {msg.value().decode('utf-8')}")

