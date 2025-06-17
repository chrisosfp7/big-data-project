from pyspark.sql import SparkSession
from datetime import datetime
from pyspark.sql.types import StructField, StructType, IntegerType, FloatType, StringType
from pyspark.sql.functions import col,hour,avg,to_timestamp,unix_timestamp, udf, max,count


spark = SparkSession \
    .builder \
    .appName("Q5 DF PARQ") \
    .getOrCreate()
sc = spark.sparkContext
username = "krkostas"

sc.setLogLevel("ERROR")
job_id = sc.applicationId
output_dir = f"hdfs://hdfs-namenode:9000/user/{username}/Q5_DF_PARQ{job_id}"

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
taxis_lookup = spark.read.parquet(f"hdfs://hdfs-namenode:9000/user/{username}/taxi_zone_lookup.parquet").select("LocationID", "Borough", "Zone")

pickup_join = taxis_df.join(
    taxis_lookup.withColumnRenamed("LocationID", "pu_locid").withColumnRenamed("Borough", "pickup_borough").withColumnRenamed("Zone", "Pickup Zone"),
    taxis_df["PULocationID"] == col("pu_locid"))


drop_join = pickup_join.join(
    taxis_lookup.withColumnRenamed("LocationID", "do_locid").withColumnRenamed("Borough", "dropoff_borough").withColumnRenamed("Zone", "Dropoff Zone"),
    pickup_join["DOLocationID"] == col("do_locid"))

same_trips = drop_join.filter((col("Pickup Zone") != col("Dropoff Zone")) & ((col("pickup_borough") != 'Unknown') & (col("pickup_borough") != 'N/A')))
final_taxis = same_trips.groupBy("Pickup Zone", "Dropoff Zone").count().withColumnRenamed("count", "TotalTrips").sort(col("TotalTrips").desc())
final_taxis.show(4)
final_taxis.coalesce(1).write.mode("overwrite").option("header", "true").csv(output_dir)