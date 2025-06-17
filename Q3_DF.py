from math import radians, sin, cos, sqrt, atan2
from pyspark.sql import SparkSession
from datetime import datetime
from pyspark.sql.types import StructField, StructType, IntegerType, FloatType, StringType
from pyspark.sql.functions import col,hour,avg,to_timestamp,unix_timestamp, udf, max,count


spark = SparkSession \
    .builder \
    .appName("Q3 df") \
    .getOrCreate()
sc = spark.sparkContext
username = "krkostas"

sc.setLogLevel("ERROR")
job_id = sc.applicationId
output_dir = f"hdfs://hdfs-namenode:9000/user/{username}/Q3_DF{job_id}"

taxis_df = spark.read.format('csv') \
    .option("header", "true") \
    .option("inferSchema", "true") \
    .load("hdfs://hdfs-namenode:9000/data/yellow_tripdata_2024.csv")


taxis_lookup = spark.read.format('csv') \
    .option("header", "true") \
    .option("inferSchema", "true") \
    .load("hdfs://hdfs-namenode:9000/data/taxi_zone_lookup.csv")

taxis_df = taxis_df.select("PULocationID", "DOLocationID")

taxis_lookup = taxis_lookup.select("LocationID", "Borough")

pickup_join = taxis_df.join(
    taxis_lookup.withColumnRenamed("LocationID", "pu_locid").withColumnRenamed("Borough", "pickup_borough"),
    taxis_df["PULocationID"] == col("pu_locid"))


drop_join = pickup_join.join(
    taxis_lookup.withColumnRenamed("LocationID", "do_locid").withColumnRenamed("Borough", "dropoff_borough"),
    pickup_join["DOLocationID"] == col("do_locid"))


same_trips = drop_join.filter((col("pickup_borough") == col("dropoff_borough")) & ((col("pickup_borough") != 'Unknown') & (col("pickup_borough") != 'N/A')))
final_trips = same_trips.groupBy("pickup_borough").agg(count("*").alias("TotalTrips")).orderBy(col("TotalTrips").desc()) \
    .withColumnRenamed("pickup_borough", "Borough")
final_trips.show()
final_trips.coalesce(1).write.format("csv").option("header", "true").save(output_dir)