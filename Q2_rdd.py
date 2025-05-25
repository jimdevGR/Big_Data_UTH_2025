from pyspark.sql import SparkSession
from math import sin, atan2, cos, sqrt, radians
from datetime import datetime

# Αρχικοποίηση spark session.
my_spark = SparkSession \
    .builder \
    .appName("Q1_rdd") \
    .getOrCreate() \
    .sparkContext 

# Ελαχιστοποίηση εξόδων καταγραφής.
my_spark.setLogLevel("ERROR")

input_file_path = "hdfs://hdfs-namenode:9000/data/yellow_tripdata_2015.csv"
output_dir_path = "hdfs://hdfs-namenode:9000/user/dikaragiannis/project_2025_outputs"

tripdata_rdd = my_spark.textFile(input_file_path)

rdd_header = tripdata_rdd.first()

# Φιλτράρισμα του dataset έτσι ΄ώστε να μην περιέχει τα ονόματα των στηλών.
tripdata_rdd = tripdata_rdd.filter(lambda line : line != rdd_header)

# Συνάρτηση για τον υπολογισμό της απόστασης haversine.
def haversine_distance(long, lat, long_, lat_): # Κάτω παύλα στις συντεταγμένες του δεύτερου σημείου.
    R = 6378
    long, lat, long_, lat_ = map(radians, [long, lat, long_, lat_])
    long_difference = (long_ - long) / 2
    lat_difference = (lat_ - lat) / 2
    a = sin(lat_difference)**2 + cos(lat) * cos(lat_) * sin(long_difference)**2
    c = 2 * atan2(sqrt(a), sqrt(1 - a))
    return R * c

# Συνάρτηση για την επεξεργασία κάθε γραμμής του csv.
def process_line(line):
    cols = line.split(",")
    vendor_id = cols[0]
    start_timestamp = cols[1] ; end_timestamp = cols[2]
    long = float(cols[5]) ; lat = float(cols[6])
    long_ = float(cols[9]) ; lat_ = float(cols[10])
    if long != 0 and lat != 0 and long_ !=0 and lat_ != 0:
        haversine_distance_ = haversine_distance(long, lat, long_, lat_)
        format = "%Y-%m-%d %H:%M:%S"
        start_datetime = datetime.strptime(start_timestamp, format)
        end_datetime = datetime.strptime(end_timestamp, format)
        trip_duration = (end_datetime - start_datetime).total_seconds() / 60 # Duration σε λεπτά
        return (vendor_id, (haversine_distance_, trip_duration))
    else:
        return None
    
distance_duration_rdd = (
    tripdata_rdd
       .map(process_line)
       .filter(lambda tuple: tuple is not None)
       .reduceByKey(lambda tuple1, tuple2: max(tuple1, tuple2, key = lambda x: x[0])) # Επιλογή του tuple με την μέγιστη απόσταση.
       .sortByKey()
)

# Δημιουργία rdd για το αποτέλεσμα.
result_header = my_spark.parallelize(["VendorID\tMax Haversine Distance (km)\tDuration (min)"])
result_rdd = distance_duration_rdd.map(lambda v : f"{v[0]}\t\t\t{v[1][0]:.2f}\t\t\t{v[1][1]:.1f}")
final_rdd = result_header.union(result_rdd)

# Αποθήκευση της εξόδου σε ένα αρχείο.
final_rdd.coalesce(1).saveAsTextFile(f"{output_dir_path}/Q2_rdd_output")

my_spark.stop()
