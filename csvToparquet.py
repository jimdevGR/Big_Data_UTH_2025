from pyspark.sql import SparkSession
from pyspark.sql.types import *

# Αρχικοποίηση spark session.
my_spark_session = SparkSession \
    .builder \
    .appName("CsvToparquetTransformation") \
    .getOrCreate() \
    
file_names = ["yellow_tripdata_2024.csv", "yellow_tripdata_2015.csv", "taxi_zone_lookup.csv"]

input_dir_path = "hdfs://hdfs-namenode:9000/data"
output_dir_path = "hdfs://hdfs-namenode:9000/user/dikaragiannis/data/parquet"

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

# Μετατροπή των .csv αρχείων σε parquet format και ανέβασμα στο hdfs.
for file_name in file_names:
    file_path = f"{input_dir_path}/{file_name}"
    if file_name == "yellow_tripdata_2024.csv":
       file_df = my_spark_session.read.option("header", True).schema(yellow_tripdata_schema).csv(file_path)
    else:
        file_df = my_spark_session.read.option("header", True).option("inferSchema", True).csv(file_path)
    file_df.write.parquet(f"{output_dir_path}/{file_name.replace('.csv', '')}")

my_spark_session.stop()
