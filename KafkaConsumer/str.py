import argparse
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, from_json, window,
    count, sum, min, max, avg, stddev
)
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, TimestampType

# -------------------------
# Argumenti aplikacije
# -------------------------
parser = argparse.ArgumentParser()
parser.add_argument("--brokers", required=True)
parser.add_argument("--topic", required=True)
parser.add_argument("--window", required=True)
parser.add_argument("--slide", required=True)
parser.add_argument("--group-by", required=True)
parser.add_argument("--threshold", type=float, required=True)
args = parser.parse_args()

# -------------------------
# Spark session
# -------------------------
spark = (
    SparkSession.builder
    .appName("Kafka Structured Streaming Consumer")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")

# -------------------------
# Schema Kafka poruke
# -------------------------
schema = StructType([
    StructField("user_id", StringType()),
    StructField("city", StringType()),
    StructField("value", DoubleType()),
    StructField("event_time", TimestampType())
])

# -------------------------
# Čitanje iz Kafka topic-a
# -------------------------
raw_df = (
    spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", args.brokers)
    .option("subscribe", args.topic)
    .option("startingOffsets", "latest")
    .load()
)

# -------------------------
# Parsiranje JSON poruke
# -------------------------
parsed_df = (
    raw_df
    .select(from_json(col("value").cast("string"), schema).alias("data"))
    .select("data.*")
)

# -------------------------
# Filter (uslov iz zadatka)
# npr. value > threshold
# -------------------------
filtered_df = parsed_df.filter(col("value") > args.threshold)

# -------------------------
# Window + agregacije
# -------------------------
agg_df = (
    filtered_df
    .groupBy(
        window(col("event_time"), args.window, args.slide),
        col(args.group_by)
    )
    .agg(
        count("*").alias("count"),
        sum("value").alias("sum_value"),
        min("value").alias("min_value"),
        max("value").alias("max_value"),
        avg("value").alias("avg_value"),
        stddev("value").alias("stddev_value")
    )
)

# -------------------------
# Output (console – može i Kafka / DB)
# -------------------------
query = (
    agg_df
    .writeStream
    .outputMode("complete")
    .format("console")
    .option("truncate", False)
    .start()
)

query.awaitTermination()
