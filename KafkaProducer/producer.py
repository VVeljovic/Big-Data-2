from confluent_kafka import Producer
from uuid import uuid4

config = {
    'bootstrap.servers': 'localhost:9092',  # Kafka server address
    'group.id': 'my-first-group',          # Consumer group name
    'auto.offset.reset': 'earliest'         # Start reading from beginning
}

producer = Producer(config)

file = open('yellow_tripdata_2015-01.csv', 'r', encoding='utf-8')
line = file.readline()
while line:
    producer.produce(
        topic="Messages",
        key=str(uuid4()),
        value=line.encode("utf-8")
    )
    producer.flush()
    line = file.readline()

file.close()