from pyspark.sql import SparkSession
from datetime import datetime
from pyspark.sql.types import StructField, StructType, IntegerType, FloatType, StringType
from pyspark.sql.functions import col,hour,avg,to_timestamp,unix_timestamp, udf, max,count,sum,round


spark = SparkSession \
    .builder \
    .appName("Q6 DF 1") \
    .getOrCreate()
sc = spark.sparkContext
username = "krkostas"

sc.setLogLevel("ERROR")
job_id = sc.applicationId
output_dir = f"hdfs://hdfs-namenode:9000/user/{username}/Q6_DF_1{job_id}"

taxis_df = spark.read.format('csv') \
    .option("header", "true") \
    .option("inferSchema", "true") \
    .load("hdfs://hdfs-namenode:9000/data/yellow_tripdata_2024.csv")

taxis_lookup = spark.read.format('csv') \
    .option("header", "true") \
    .option("inferSchema", "true") \
    .load("hdfs://hdfs-namenode:9000/data/taxi_zone_lookup.csv")

taxis_df = taxis_df.select("PULocationID", "DOLocationID", "fare_amount","tip_amount","tolls_amount","extra","mta_tax","congestion_surcharge","Airport_fee","total_amount")
taxis_lookup = taxis_lookup.select("LocationID", "Borough")


pickup_join = taxis_df.join(
    taxis_lookup.withColumnRenamed("LocationID", "pu_locid").withColumnRenamed("Borough", "pickup_borough"),
    taxis_df["PULocationID"] == col("pu_locid"))


drop_join = pickup_join.join(
    taxis_lookup.withColumnRenamed("LocationID", "do_locid").withColumnRenamed("Borough", "dropoff_borough"),
    pickup_join["DOLocationID"] == col("do_locid"))

same_trips = drop_join.filter(((col("pickup_borough") != 'Unknown') & (col("pickup_borough") != 'N/A'))).select("pickup_borough","fare_amount","tip_amount","tolls_amount","extra","mta_tax","congestion_surcharge","Airport_fee","total_amount")
final_taxis = same_trips.groupBy("pickup_borough").agg(
    round(sum("fare_amount"),2).alias("Fare($)"), round(sum("tip_amount"),2).alias("Tips($)"), round(sum("tolls_amount"),2).alias("Tols($)"), round(sum("extra"),2).alias("extras($)"),
    round(sum("mta_tax"),2).alias("MTA TAX($)"), round(sum("congestion_surcharge"),2).alias("Congestion($)"),
    round(sum("Airport_fee"),2).alias("Airport fee($)"), round(sum("total_amount"),2).alias("Total Revenue($)")
)
final_taxis.show()
final_taxis.coalesce(1).write.mode("overwrite").option("header", "true").csv(output_dir)