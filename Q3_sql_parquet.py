from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import col

# Αρχικοποίηση spark session.
my_spark_session = SparkSession \
    .builder \
    .appName("Q3_sql_parquet_input") \
    .getOrCreate() \

# Αρχικοποίηση spark context.   
my_spark_context = my_spark_session.sparkContext

# Ελαχιστοποίηση εξόδων καταγραφής.
my_spark_context.setLogLevel("ERROR")

input_file_path_2024 = "hdfs://hdfs-namenode:9000/user/dikaragiannis/data/parquet/yellow_tripdata_2024"
input_file_path_lookup = "hdfs://hdfs-namenode:9000/user/dikaragiannis/data/parquet/taxi_zone_lookup"
output_dir_path = "hdfs://hdfs-namenode:9000/user/dikaragiannis/project_2025_outputs"

try:

    tripdata_df = my_spark_session.read.parquet(input_file_path_2024)
    lookup_df = my_spark_session.read.parquet(input_file_path_lookup)

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

    total_trips_df.coalesce(1).write.mode("overwrite").option("header", True).csv(f"{output_dir_path}/Q3_sql_parquetInput_output")

except Exception as e:
    print("An error occured!", e)
    raise

finally:
    my_spark_session.stop()
