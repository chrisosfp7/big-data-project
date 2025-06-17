from pyspark.sql import SparkSession

sc = SparkSession \
    .builder \
    .appName("Q1") \
    .getOrCreate() \
    .sparkContext
username = "krkostas"

sc.setLogLevel("ERROR")
job_id = sc.applicationId
output_dir = f"hdfs://hdfs-namenode:9000/user/{username}/Q1_{job_id}"


taxis = sc.textFile("hdfs://hdfs-namenode:9000/data/yellow_tripdata_2015.csv").map(lambda x: x.split(",")).filter(lambda x: x[5]!= '0' and x[6] != '0')
taxis_formated = taxis.filter(lambda x:len(x) > 6 and " " in x[1]) \
    .map(lambda x: [x[1].split(" ")[1].split(":")[0], [float(x[5]), float(x[6])]]).sortByKey()


grouped_taxis = taxis_formated.groupByKey()


def mesi_ora(arr):
    sid = list(arr)
    sid = [(float(x[0]), float(x[1])) for x in sid]
    log = sum(x for x, y in sid)
    lat = sum(y for x, y in sid)
    avg_1 = log / len(sid)
    avg_2 = lat / len(sid)
    return [(avg_1, avg_2)]


final_taxi = grouped_taxis.flatMapValues(mesi_ora).sortByKey()
print("HourOfDay\tLongitude\tLatitude")
for hour, (log, lat) in final_taxi.collect():
    print(f"{hour}: {log}, {lat}")


final_taxi.coalesce(1).saveAsTextFile(output_dir)