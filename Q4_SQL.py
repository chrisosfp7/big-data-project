from pyspark.sql import SparkSession
from datetime import datetime
from pyspark.sql.functions import col,hour,avg,to_timestamp,unix_timestamp, udf, max,count

spark = SparkSession \
    .builder \
    .appName("Q4 SQL") \
    .getOrCreate()
sc = spark.sparkContext
username = "krkostas"

sc.setLogLevel("ERROR")
job_id = sc.applicationId
output_dir = f"hdfs://hdfs-namenode:9000/user/{username}/Q4_SQL{job_id}"

taxis_df = spark.read.format('csv') \
    .option("header", "true") \
    .option("inferSchema", "true") \
    .load("hdfs://hdfs-namenode:9000/data/yellow_tripdata_2024.csv")

taxis_df.createOrReplaceTempView("night_trips")
query_1 = """
    SELECT VendorID, COUNT(*) as NightTrips
    FROM night_trips
    WHERE HOUR(tpep_pickup_datetime) >= 23 OR HOUR(tpep_pickup_datetime) < 7
    GROUP BY VendorID
"""

final_taxis_sql = spark.sql(query_1)
final_taxis_sql.show()
final_taxis_sql.coalesce(1).write.mode("overwrite").option("header", "true").csv(output_dir)
