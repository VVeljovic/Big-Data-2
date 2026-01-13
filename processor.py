from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, DoubleType
from pyspark.sql.functions import from_csv, col, count, lower, trim, avg, stddev, window, sum, min, max, to_json, struct
import sys

from pyspark.sql.types import (
    StructType, StructField,
    IntegerType, DoubleType,
    StringType, TimestampType
)

window_duration = sys.argv[1]      
slide_duration = sys.argv[2]      
window_type = sys.argv[3]          
filter_column = sys.argv[4]       
filter_value = int(sys.argv[5])   
group_column = sys.argv[6]         
agg_column = sys.argv[7]  

spark = SparkSession.builder.appName("Big-Data-2").getOrCreate()
spark.sparkContext.setLogLevel("ERROR")

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


df = spark.readStream.format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "initial_data") \
    .load()

raw_df = df.selectExpr("CAST(value AS STRING) as csv_line")

parsed_df = (
    raw_df
    .select(from_csv(col("csv_line"), schema_ddl).alias("data"))
    .select("data.*")
)

filtered_df = parsed_df.filter(col(filter_column) > filter_value)

if window_type == "tumbling":
    windowed_col = window(col("tpep_pickup_datetime"), window_duration)
else:
    windowed_col = window(col("tpep_pickup_datetime"), window_duration, slide_duration)
    

agg_df = filtered_df.withWatermark("tpep_pickup_datetime", "30 minutes") \
    .groupBy(windowed_col, col(group_column)) \
    .agg(
        count("*").alias("count"),
        sum(agg_column).alias("sum"),
        min(agg_column).alias("min"),
        max(agg_column).alias("max"),
        avg(agg_column).alias("avg"),
        stddev(agg_column).alias("stddev")
    )

non_empty_df = agg_df.filter(col("count") > 0)

json_df = non_empty_df.select(to_json(struct("*")).alias("value"))  


query = json_df.writeStream \
    .outputMode("update") \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("topic", "aggregated_results") \
    .option("checkpointLocation", "/tmp/spark_checkpoint") \
    .start()

query.awaitTermination()