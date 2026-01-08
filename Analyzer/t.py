import sys
import argparse
from confluent_kafka import Consumer
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_csv, split
from Accident import Accident

def parse_csv_to_accident(csv_row):
    cols = csv_row.split(",")
    return Accident(
        ID=cols[0],
        Source=cols[1],
        Severity=cols[2],
       # Start_Time=cols[3],
        #End_Time=cols[4],
        Start_Lat=cols[5],
        Start_Lng=cols[6],
        End_Lat=cols[7],
        End_Lng=cols[8],
        Distance_mi=cols[9],
        Description=cols[10],
        Street=cols[11],
        City=cols[12],
        County=cols[13],
        State=cols[14],
        Zipcode=cols[15],
        Country=cols[16],
        Timezone=cols[17],
        Airport_Code=cols[18],
        Weather_Timestamp=cols[19],
        Temperature_F=cols[20],
        Wind_Chill_F=cols[21],
        Humidity=cols[22],
        Pressure=cols[23],
        Visibility=cols[24],
        Wind_Direction=cols[25],
        Wind_Speed_mph=cols[26],
        Precipitation_in=cols[27],
        Weather_Condition=cols[28]
    )

parser = argparse.ArgumentParser()
parser.add_argument("--topic", required=True)
parser.add_argument("--window", required=True)
parser.add_argument("--slide", required=True)
parser.add_argument("--severity", type=int, required=True)
parser.add_argument("--group-by", required=True)
args = parser.parse_args()

spark = SparkSession.builder.appName("Kafka Structured Streaming Consumer").getOrCreate()

config = {
    'bootstrap.servers': 'localhost:9092',  # Kafka server address
    'group.id': 'my-first-group',          # Consumer group name
    'auto.offset.reset': 'earliest'         # Start reading from beginning
}

raw_df = spark.readStream.format("kafka") \
    .option("kafka.bootstrap.servers", 'localhost:9092') \
    .option("subscribe", args.topic) \
    .option("startingOffsets", "latest") \
    .load()

accidents_df = raw_df.selectExpr("CAST(value AS STRING) as csv")

def process_batch(df, epoch_id):
    rows = df.select("csv").collect()
    accidents = [parse_csv_to_accident(row["csv"]) for row in rows]
    for acc in accidents:
        print(acc)  

query = accidents_df.writeStream.foreachBatch(process_batch).start()
query.awaitTermination()

