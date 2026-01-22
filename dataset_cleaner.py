from pyspark.sql import SparkSession
spark = SparkSession.builder \
    .appName("DatasetCleaner") \
    .master("local[*]") \
    .getOrCreate()

df = spark.read.csv("yellow_tripdata_2015-01.csv", header=True, inferSchema=True)

df = df.dropna()

df.coalesce(1).write.csv("final_output", header=True, mode="overwrite")