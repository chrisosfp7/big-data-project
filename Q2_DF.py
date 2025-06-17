from math import radians, sin, cos, sqrt, atan2
from pyspark.sql import SparkSession
from datetime import datetime
from pyspark.sql.types import StructField, StructType, IntegerType, FloatType, StringType
from pyspark.sql.functions import col,hour,avg,to_timestamp,unix_timestamp, udf, max


spark = SparkSession \
    .builder \
    .appName("Q2 df") \
    .getOrCreate()
sc = spark.sparkContext
username = "krkostas"

sc.setLogLevel("ERROR")
job_id = sc.applicationId
output_dir = f"hdfs://hdfs-namenode:9000/user/{username}/Q2_DF{job_id}"

taxis_df = spark.read.format('csv') \
    .option("header", "true") \
    .option("inferSchema", "true") \
    .load("hdfs://hdfs-namenode:9000/data/yellow_tripdata_2015.csv")

taxis_df_up = taxis_df.select("VendorID", "tpep_dropoff_datetime", "tpep_pickup_datetime", "pickup_longitude", "pickup_latitude","dropoff_longitude", "dropoff_latitude")
filtered = taxis_df_up.filter((col("pickup_longitude") != 0) & (col("pickup_latitude") != 0) & (col("dropoff_longitude") != 0) & (col("dropoff_latitude") != 0))
filtered = filtered.withColumn("pickup_ts", to_timestamp("tpep_pickup_datetime")) \
       .withColumn("dropoff_ts", to_timestamp("tpep_dropoff_datetime"))
filtered_time = filtered.withColumn("Duration(min)", (unix_timestamp("dropoff_ts") - unix_timestamp("pickup_ts")) / 60)

def haversine_udf(lon1, lat1, lon2, lat2):
    R = 6371
    lon1, lat1, lon2, lat2 = map(radians, [lon1, lat1, lon2, lat2])
    dlon = lon2 - lon1
    dlat = lat2 - lat1
    a = sin(dlat/2)**2 + cos(lat1)*cos(lat2)*sin(dlon/2)**2
    return float(R * 2 * atan2(sqrt(a), sqrt(1 - a)))

haversin_apostasi = udf(haversine_udf, FloatType())
filtered_diad = filtered_time.withColumn("apostasi", haversin_apostasi(
    col("pickup_longitude"), col("pickup_latitude"),col("dropoff_longitude"), col("dropoff_latitude")
)).filter((col("apostasi") < 50) & (col("Duration(min)") > 10) )

max_dist = filtered_diad.groupBy(col("VendorID").alias("Vendor")).agg(
    max("apostasi").alias("Max Haversine DIstance(km)")
)

result = filtered_diad.join(max_dist, (filtered_diad["VendorID"] == max_dist["Vendor"]) &
                          (filtered_diad["apostasi"] == max_dist["Max Haversine DIstance(km)"]))

final_result = result.select(col("VendorId"), col("Max Haversine DIstance(km)"), col("Duration(min)"))
final_result.show()

final_result.coalesce(1).write.format("csv").option("header", "true").save(output_dir)