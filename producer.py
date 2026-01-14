from confluent_kafka.admin import AdminClient, NewTopic
from confluent_kafka import Producer
from uuid import uuid4
import time 
config = {
    "bootstrap.servers": 'localhost:9092'
}
topics_to_create = ["initial_data", "aggregated_results"]
new_topics = [NewTopic(topic, num_partitions=3, replication_factor=1) for topic in topics_to_create]

admin_client = AdminClient(config)

fs = admin_client.create_topics(new_topics) 

producer = Producer(config)

file = open('yellow_tripdata_2015-01.csv', 'r', encoding='utf-8')

line = file.readline() 
while line:
    producer.produce(
        topic="initial_data",
        key=str(uuid4()),
        value=line.encode("utf-8")
    )
    print(line)
    producer.flush()
    line = file.readline()
    time.sleep(3)

file.close()