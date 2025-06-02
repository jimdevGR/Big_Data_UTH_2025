from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import col, desc

# Αρχικοποίηση spark session.
my_spark_session = SparkSession \
    .builder \
    .appName("Q5_df_csv_input") \
    .getOrCreate() \

# Αρχικοποίηση spark context.   
my_spark_context = my_spark_session.sparkContext

# Ελαχιστοποίηση εξόδων καταγραφής.
my_spark_context.setLogLevel("ERROR")

input_file_path_2024 = "hdfs://hdfs-namenode:9000/data/yellow_tripdata_2024.csv"
input_file_path_lookup = "hdfs://hdfs-namenode:9000/data/taxi_zone_lookup.csv"
output_dir_path = "hdfs://hdfs-namenode:9000/user/dikaragiannis/project_2025_outputs"

yellow_tripdata_schema = StructType([
    StructField(name = "VendorID", dataType = IntegerType()),
    StructField(name = "tpep_pickup_datetime", dataType = TimestampType()),
    StructField(name = "tpep_dropoff_datetime", dataType = TimestampType()),
    StructField(name = "passenger_count", dataType = IntegerType()),
    StructField(name = "trip_distance", dataType = DoubleType()),
    StructField(name = "RatecodeID", dataType = IntegerType()),
    StructField(name = "store_and_fwd_flag", dataType = StringType()),
    StructField(name = "PULocationID", dataType = IntegerType()),
    StructField(name = "DOLocationID", dataType = IntegerType()),
    StructField(name = "payment_type", dataType = IntegerType()),
    StructField(name = "fare_amount", dataType = DoubleType()),
    StructField(name = "extra", dataType = DoubleType()),
    StructField(name = "mta_tax", dataType = DoubleType()),
    StructField(name = "tip_amount", dataType = DoubleType()),
    StructField(name = "tolls_amount", dataType = DoubleType()),
    StructField(name = "improvement_surcharge", dataType = DoubleType()),
    StructField(name = "total_amount", dataType = DoubleType()),
    StructField(name = "congestion_surcharge", dataType = DoubleType()),
    StructField(name = "airport_fee", dataType = DoubleType()),
    StructField(name = "cbd_congestion_fee", dataType = DoubleType())
])

lookup_data_schema = StructType([
    StructField(name = "LocationID", dataType = IntegerType()),
    StructField(name = "Borough", dataType = StringType()),
    StructField(name = "Zone", dataType = StringType()),
    StructField(name = "service_zone", dataType = StringType())
])

tripdata_df = my_spark_session.read.option("header", True).schema(yellow_tripdata_schema).csv(input_file_path_2024).select(col("PULocationID"), col("DOLocationID"))
lookup_df = my_spark_session.read.option("header", True).schema(lookup_data_schema).csv(input_file_path_lookup).select(col("Zone"), col("LocationID"))

# Υπολογισμός των διαδρομών για κάθε ζεύγος διαφορετικών ζωνών.
zone_pairs_df = (
    tripdata_df
       .alias("trips")
       .join(lookup_df.alias("pickup"), col("trips.PULocationID") == col("pickup.LocationID"))
       .join(lookup_df.alias("dropoff"), col("trips.DOLocationID") == col("dropoff.LocationID"))
       .select(col("pickup.Zone").alias("Pickup Zone"), col("dropoff.Zone").alias("Dropoff Zone"))
       .filter(col("Pickup Zone") != col("Dropoff Zone"))
       .groupBy("Pickup Zone", "Dropoff Zone")
       .count()
       .withColumnRenamed("count", "TotalTrips")
       .orderBy(desc("TotalTrips"))
)

# Εμφάνιση των 4 με τις περισσότερες διαδρομές όπως ζητείται και στην εκφώνηση.
zone_pairs_df.show(4, truncate = False)

zone_pairs_df.coalesce(1).write.mode("overwrite").option("header", True).csv(f"{output_dir_path}/Q5_df_csvInput_output")
                                                                              
my_spark_session.stop()