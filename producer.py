import os
from confluent_kafka import Producer
from uuid import uuid4

KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")

config = {
    "bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS
}

producer = Producer(config)

file = open('yellow_tripdata_2015-01.csv', 'r', encoding='utf-8')

line = file.readline()

while line:
    producer.produce(
        topic="inital_data",
        key=str(uuid4()),
        value=line.encode("utf-8")
    )
    producer.flush()
    line = file.readline()

file.close()