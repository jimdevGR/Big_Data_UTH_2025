from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import col

# Αρχικοποίηση spark session.
my_spark_session = SparkSession \
    .builder \
    .appName("Q4_sql_parquet_input") \
    .getOrCreate() \

# Αρχικοποίηση spark context.   
my_spark_context = my_spark_session.sparkContext

# Ελαχιστοποίηση εξόδων καταγραφής.
my_spark_context.setLogLevel("ERROR")

input_file_path_2024 = "hdfs://hdfs-namenode:9000/user/dikaragiannis/data/parquet/yellow_tripdata_2024"
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

tripdata_df = my_spark_session.read.parquet(input_file_path_2024).select(col("tpep_pickup_datetime"), col("VendorID"))

tripdata_df.createOrReplaceTempView("tripdata_table")

# Ερώτημα για τον υπολογισμό των νυχτερινών διαδρομών ταξί.
my_query = """
            SELECT VendorID, COUNT(*) AS NightTrips
            FROM (
                 SELECT VendorID, HOUR (tpep_pickup_datetime) AS hour
                 FROM tripdata_table
                 ) id_hour
            WHERE hour >= 23 OR hour < 7
            GROUP BY VendorID
            ORDER BY VendorID
"""

# Εκτέλεση του ερωτήματος.
night_trips_df = my_spark_session.sql(my_query)

night_trips_df.show(truncate = False)

night_trips_df.coalesce(1).write.mode("overwrite").option("header", True).csv(f"{output_dir_path}/Q4_sql_parquetInput_output")

my_spark_session.stop()