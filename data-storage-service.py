import json
from confluent_kafka import Consumer
from uuid import uuid4
from cassandra.cluster import Cluster

cluster = Cluster(['localhost'])
session = cluster.connect()

session.execute("""
    CREATE KEYSPACE IF NOT EXISTS taxi
    WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1}
""")

session.set_keyspace('taxi')

session.execute("""
    CREATE TABLE IF NOT EXISTS aggregated_taxi (
        window_start timestamp,
        window_end timestamp,
        group_column_name text,
        group_column_value int,
        condition text,
        count int,
        sum double,
        min double,
        max double,
        avg double,
        stddev double,
        variance double,
        median double,
        PRIMARY KEY ((window_start, window_end), min)    
    )
""")

config = {
    "bootstrap.servers": 'localhost:9092',
    "group.id": f"consumer-{uuid4()}",  
    "auto.offset.reset": "earliest"    
}

consumer = Consumer(config)

consumer.subscribe(["aggregated_results"])

def insert_aggregated_record(data, session):
    session.execute(
        """
        INSERT INTO aggregated_taxi (
            window_start, window_end, group_column_value, group_column_name,
            count, sum, min, max, avg, stddev, variance, median, condition
        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """,
        (
            data.get("window", {}).get("start", None),
            data.get("window", {}).get("end", None),
            data.get("group_value", None),
            data.get("group_column_name", None),
            data.get("count", None),
            data.get("sum", None),
            data.get("min", None),
            data.get("max", None),
            data.get("avg", None),
            data.get("stddev", None),
            data.get("variance", None),
            data.get("median", None),
            data.get("condition", None)
        )
    )

try:
    while True:
        msg = consumer.poll(1.0)
        if msg is None:
            continue
        if msg.error():
            print(f"Consumer error: {msg.error()}")
            continue

        message = msg.value().decode('utf-8')
        print(f"Received message: {message}")

        data = json.loads(message)
        insert_aggregated_record(data, session)

finally:
    consumer.close()
    print("Consumer closed")



