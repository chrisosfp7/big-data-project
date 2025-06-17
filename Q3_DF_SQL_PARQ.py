from math import radians, sin, cos, sqrt, atan2
from pyspark.sql import SparkSession
from datetime import datetime
from pyspark.sql.types import StructField, StructType, IntegerType, FloatType, StringType
from pyspark.sql.functions import col,hour,avg,to_timestamp,unix_timestamp, udf, max,count



spark = SparkSession \
    .builder \
    .appName("Q3 df SQL PARQ") \
    .getOrCreate()
sc = spark.sparkContext
username = "krkostas"

sc.setLogLevel("ERROR")
job_id = sc.applicationId
output_dir = f"hdfs://hdfs-namenode:9000/user/{username}/Q3_DF_SQL_PARQ{job_id}"

taxis_df = spark.read.format('csv') \
    .option("header", "true") \
    .option("inferSchema", "true") \
    .load("hdfs://hdfs-namenode:9000/data/yellow_tripdata_2024.csv")


taxis_lookup = spark.read.format('csv') \
    .option("header", "true") \
    .option("inferSchema", "true") \
    .load("hdfs://hdfs-namenode:9000/data/taxi_zone_lookup.csv")

taxis_df.write.mode("overwrite").parquet(f"hdfs://hdfs-namenode:9000/user/{username}/yellow_tripdata_2024.parquet")
taxis_lookup.write.mode("overwrite").parquet(f"hdfs://hdfs-namenode:9000/user/{username}/taxi_zone_lookup.parquet")

taxis_df = spark.read.parquet(f"hdfs://hdfs-namenode:9000/user/{username}/yellow_tripdata_2024.parquet").select("PULocationID", "DOLocationID")
taxis_lookup = spark.read.parquet(f"hdfs://hdfs-namenode:9000/user/{username}/taxi_zone_lookup.parquet").select("LocationID", "Borough")

taxis_df.createOrReplaceTempView("trips")
taxis_lookup.createOrReplaceTempView("zones")

query_1 = """
  SELECT
  pickup.Borough AS Borough,
  COUNT(*) AS TotalTrips
FROM trips 
JOIN zones pickup ON trips.PULocationID = pickup.LocationID
JOIN zones dropoff ON trips.DOLocationID = dropoff.LocationID
WHERE pickup.Borough NOT IN ('Unknown', 'N/A')
  AND pickup.Borough = dropoff.Borough
GROUP BY pickup.Borough
ORDER BY
  TotalTrips DESC
"""
final_taxis_sql = spark.sql(query_1)
final_taxis_sql.show()
final_taxis_sql.coalesce(1).write.mode("overwrite").option("header", "true").csv(output_dir)