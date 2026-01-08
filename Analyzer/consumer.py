import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, DoubleType
from pyspark.sql.functions import from_csv, col, count, lower, trim, avg, stddev, window, sum, min, max, to_json, struct
import json
from pyspark.sql.types import (
    StructType, StructField,
    IntegerType, DoubleType,
    StringType, TimestampType
)

taxi_schema = StructType([
    StructField("VendorID", IntegerType(), True),

    StructField("tpep_pickup_datetime", TimestampType(), True),
    StructField("tpep_dropoff_datetime", TimestampType(), True),

    StructField("passenger_count", IntegerType(), True),
    StructField("trip_distance", DoubleType(), True),

    StructField("pickup_longitude", DoubleType(), True),
    StructField("pickup_latitude", DoubleType(), True),

    StructField("RateCodeID", IntegerType(), True),

    StructField("store_and_fwd_flag", StringType(), True),

    StructField("dropoff_longitude", DoubleType(), True),
    StructField("dropoff_latitude", DoubleType(), True),

    StructField("payment_type", IntegerType(), True),
    StructField("fare_amount", DoubleType(), True),
    StructField("extra", DoubleType(), True),
    StructField("mta_tax", DoubleType(), True),
    StructField("tip_amount", DoubleType(), True),
    StructField("tolls_amount", DoubleType(), True),
    StructField("improvement_surcharge", DoubleType(), True),
    StructField("total_amount", DoubleType(), True)
])
schema_ddl = taxi_schema.simpleString()

spark = SparkSession.builder.appName("Kafka Consumer").getOrCreate()
spark.sparkContext.setLogLevel("ERROR")

df = spark.readStream.format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "Messages") \
    .load()

raw_df = df.selectExpr("CAST(value AS STRING) as csv_line")


parsed_df = (
    raw_df
    .select(from_csv(col("csv_line"), schema_ddl).alias("data"))
    .select("data.*")
)

filtered_df = (
    parsed_df
    #.filter((col("trip_distance") > 3) & (col("total_amount") >= 10))
)

agg_df = (
    filtered_df
    
    .groupBy(
        window(col("tpep_pickup_datetime"), "3 seconds"),
        col("VendorID")
    )
    .agg(
        count("*").alias("count"),
        sum("trip_distance").alias("sum_distance"),
        min("trip_distance").alias("min_distance"),
        max("trip_distance").alias("max_distance"),
        avg("trip_distance").alias("avg_distance"),
        stddev("trip_distance").alias("stddev_distance")
    )
)

kafka_df = agg_df.select(
    to_json(struct(
        col("window"),
        col("VendorID"),
        col("count"),
        col("sum_distance"),
        col("min_distance"),
        col("max_distance"),
        col("avg_distance"),
        col("stddev_distance")
    )).alias("value")
)

query = (
    kafka_df
    .writeStream
    .format("kafka")
    .option("kafka.bootstrap.servers", "localhost:9092")
    .option("topic", "AggregatedMessages")
    .option("checkpointLocation", "/tmp/kafka_checkpoint/")
    .outputMode("update")
    .start()
)

query_console = (
    kafka_df
    .writeStream
    .outputMode("update")
    .format("console")
    .option("truncate", False)
    .start()
)

query_console.awaitTermination()
query.awaitTermination()