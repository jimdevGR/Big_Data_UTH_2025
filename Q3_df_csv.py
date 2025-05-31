from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import col, desc

# Αρχικοποίηση spark session.
my_spark_session = SparkSession \
    .builder \
    .appName("Q3_dataframe_csv_input") \
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
lookup_df = my_spark_session.read.option("header", True).schema(lookup_data_schema).csv(input_file_path_lookup).select(col("LocationID"), col("Borough"))

boroughs = ["Manhattan", "Queens", "Brooklyn", "Bronx", "Staten Island"]

# Υπολογισμός ζητούμενου dataframe.
total_trips_df = (
    tripdata_df
        .join(lookup_df, tripdata_df.PULocationID == lookup_df.LocationID)
        .withColumnRenamed("Borough", "Borough_enter")
        .drop("LocationID")
        .join(lookup_df, tripdata_df.DOLocationID == lookup_df.LocationID)
        .withColumnRenamed("Borough", "Borough_drop")
        .drop("LocationID")
        .filter((col("Borough_enter") == col("Borough_drop")) & (col("Borough_enter").isin(*boroughs)))
        .groupBy("Borough_enter")
        .count()
        .withColumnRenamed("Borough_enter", "Borough")
        .withColumnRenamed("count", "TotalTrips")
        .orderBy(desc("TotalTrips"))
)

total_trips_df.show(truncate = False)

total_trips_df.coalesce(1).write.mode("overwrite").option("header", True).csv(f"{output_dir_path}/Q3_df_csv_output")

my_spark_session.stop()


