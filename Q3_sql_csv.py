from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import col

# Αρχικοποίηση spark session.
my_spark_session = SparkSession \
    .builder \
    .appName("Q3_sql_csv_input") \
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

try:

    tripdata_df = my_spark_session.read.option("header", True).schema(yellow_tripdata_schema).csv(input_file_path_2024).select(col("PULocationID"), col("DOLocationID"))
    lookup_df = my_spark_session.read.option("header", True).schema(lookup_data_schema).csv(input_file_path_lookup).select(col("LocationID"), col("Borough"))

    boroughs = ["Manhattan", "Queens", "Brooklyn", "Bronx", "Staten Island"]

    # Δημιουργία προσωρινών πινάκων.
    tripdata_df.createOrReplaceTempView("tripdata_table")
    lookup_df.createOrReplaceTempView("lookup_table")

    # Ερώτημα για τον υπολογισμό των διαδρομών που ξεκίνησαν και τελείωσαν στον ίδιο δήμο.
    my_query = f"""
                SELECT b_enter AS Borough, COUNT(*) AS TotalTrips
                FROM
                    (
                     SELECT look_enter.borough AS b_enter, look_drop.borough AS b_drop
                     FROM tripdata_table AS trips 
                     JOIN lookup_table AS look_enter ON trips.PULocationID = look_enter.LocationID
                     JOIN lookup_table AS look_drop ON trips.DOLocationID = look_drop.LocationID
                    ) joined_data
                WHERE joined_data.b_enter = joined_data.b_drop
                AND joined_data.b_enter IN ({','.join([f"'{borough}'" for borough in boroughs])})
                GROUP BY b_enter
                ORDER BY TotalTrips DESC
    """

    # Εκτέλεση του ερωτήματος.
    total_trips_df = my_spark_session.sql(my_query)

    total_trips_df.show(truncate = False)

    total_trips_df.coalesce(1).write.mode("overwrite").option("header", True).csv(f"{output_dir_path}/Q3_sql_csvInput_output")

except Exception as e:
    print("An error occured!", e)
    raise

finally:
    my_spark_session.stop()
