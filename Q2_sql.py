from pyspark.sql import SparkSession
from pyspark.sql.types import *
from math import sin, atan2, cos, sqrt, radians
from pyspark.sql.functions import col, udf

# Αρχικοποίηση spark session.
my_spark_session = SparkSession \
    .builder \
    .appName("Q2_sql") \
    .getOrCreate() \

# Αρχικοποίηση spark context.   
my_spark_context = my_spark_session.sparkContext

# Ελαχιστοποίηση εξόδων καταγραφής.
my_spark_context.setLogLevel("ERROR")

input_file_path = "hdfs://hdfs-namenode:9000/data/yellow_tripdata_2015.csv"
output_dir_path = "hdfs://hdfs-namenode:9000/user/dikaragiannis/project_2025_outputs"

yellow_tripdata_schema = StructType([
    StructField(name = "VendorID", dataType = IntegerType()),
    StructField(name = "tpep_pickup_datetime", dataType = TimestampType()),
    StructField(name = "tpep_dropoff_datetime", dataType = TimestampType()),
    StructField(name = "passenger_count", dataType = IntegerType()),
    StructField(name = "trip_distance", dataType = DoubleType()),
    StructField(name = "pickup_longitude", dataType = DoubleType()),
    StructField(name = "pickup_latitude", dataType = DoubleType()),
    StructField(name = "RateCodeID", dataType = IntegerType()),
    StructField(name = "store_and_fwd_flag", dataType = StringType()),
    StructField(name = "dropoff_longitude", dataType = DoubleType()),
    StructField(name = "dropoff_latitude", dataType = DoubleType()),
    StructField(name = "payment_type", dataType = IntegerType()),
    StructField(name = "fare_amount", dataType = DoubleType()),
    StructField(name = "extra", dataType = DoubleType()),
    StructField(name = "mta_tax", dataType = DoubleType()),
    StructField(name = "tip_amount", dataType = DoubleType()),
    StructField(name = "tolls_amount", dataType = DoubleType()),
    StructField(name = "improvement_surcharge", dataType = DoubleType()),
    StructField(name = "total_amount", dataType = DoubleType())
    ])

tripdata_df = my_spark_session.read.option("header", True).schema(yellow_tripdata_schema).csv(input_file_path)

# Συνάρτηση για τον υπολογισμό της απόστασης haversine.
def haversine_distance(long, lat, long_, lat_): # Κάτω παύλα στις συντεταγμένες του δεύτερου σημείου.
    R = 6378
    long, lat, long_, lat_ = map(radians, [long, lat, long_, lat_])
    long_difference = (long_ - long) / 2
    lat_difference = (lat_ - lat) / 2
    a = sin(lat_difference)**2 + cos(lat) * cos(lat_) * sin(long_difference)**2
    c = 2 * atan2(sqrt(a), sqrt(1 - a))
    return R * c

# Καταχώρηση udf
haversine_distance_udf = udf(haversine_distance, DoubleType())

# Εξαγωγή στηλών.
# Χρ΄ήση cast("long") στα timestamps για εξαγωγή δευτερολέπτων.
long = col("pickup_longitude") ; lat = col("pickup_latitude")
long_ = col("dropoff_longitude") ; lat_ = col("dropoff_latitude")
start_seconds = col("tpep_pickup_datetime").cast("long") ; end_seconds = col("tpep_dropoff_datetime").cast("long")

# Φιλτράρισμα μηδενικών τιμών, υπολογισμός αποστάσεων και λεπτών.
distance_duration_df = (
    tripdata_df
        .filter((long != 0) & (lat != 0) & (long_ != 0) & (lat_ != 0))
        .withColumn("Haversine_Distance", haversine_distance_udf(long, lat, long_, lat_))
        .withColumn("Duration_min", (end_seconds - start_seconds) / 60)
)

# Δημιουργία προσωρινού πίνακα του dataframe για την εκτέλεση ερωτημάτων sql.
distance_duration_df.createOrReplaceTempView("distance_duration_table")

my_query = """
        SELECT 
            max_dist_table.VendorID,
            ROUND (max_dist_table.max_distance, 2) AS `Max Haversine Distance (km)`,
            ROUND (distance_duration_table.Duration_min, 1) AS `Duration (min)`
        FROM 
           (
            SELECT VendorID, MAX(Haversine_Distance) AS max_distance
            FROM distance_duration_table
            GROUP BY VendorID
           ) AS max_dist_table
        JOIN distance_duration_table
        ON distance_duration_table.VendorID = max_dist_table.VendorID
        AND distance_duration_table.Haversine_Distance = max_dist_table.max_distance
        ORDER BY max_dist_table.VendorID
"""

# Εκτέλεση του ερωτήματος.
final_df = my_spark_session.sql(my_query)

final_df.show(truncate = False)

final_df.coalesce(1).write.mode("overwrite").option("header", True).csv(f"{output_dir_path}/Q2_sql_output")

my_spark_session.stop()

