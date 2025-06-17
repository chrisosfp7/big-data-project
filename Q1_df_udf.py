from pyspark.sql import SparkSession
from pyspark.sql.types import StructField, StructType, IntegerType, FloatType, StringType
from pyspark.sql.functions import col,hour,avg,udf,collect_list, struct



spark = SparkSession \
    .builder \
    .appName("Q1 df_udf") \
    .getOrCreate()
sc = spark.sparkContext
username = "krkostas"

sc.setLogLevel("ERROR")
job_id = sc.applicationId
output_dir = f"hdfs://hdfs-namenode:9000/user/{username}/Q1_df_udf{job_id}"

taxis_df = spark.read.format('csv') \
    .option("header", "true") \
    .option("inferSchema", "true") \
    .load("hdfs://hdfs-namenode:9000/data/yellow_tripdata_2015.csv")

taxis_df_up = taxis_df.select("tpep_pickup_datetime", "pickup_longitude", "pickup_latitude")
filtered = taxis_df_up.filter((col("pickup_longitude") != 0) & (col("pickup_latitude") != 0))
df_with_hour = filtered.withColumn("hour", hour(col("tpep_pickup_datetime")))
df_with_hour_up =  df_with_hour.select("hour", "pickup_longitude", "pickup_latitude")
grouped_df = df_with_hour_up.groupBy("hour").agg(
    collect_list(struct("pickup_longitude", "pickup_latitude")).alias("sidetag")
)


def calculate_longitude(sid):
    total = sum([row['pickup_longitude'] for row in sid])
    return total / len(sid) if sid else None

def calculate_latitude(sid):
    total = sum([row['pickup_latitude'] for row in sid])
    return total / len(sid) if sid else None

calculate_longitude_udf = udf(calculate_longitude, FloatType())
calculate_latitude_udf = udf(calculate_latitude, FloatType())


result = grouped_df.withColumn("Longitude", calculate_longitude_udf(col("sidetag"))) \
                   .withColumn("Latitude", calculate_latitude_udf(col("sidetag"))) \
                   .select(col("hour").alias("HourOfDay"), "Longitude", "Latitude") \
                   .orderBy("HourOfDay")


result.show()
result.coalesce(1).write.format("csv").option("header", "true").save(output_dir)