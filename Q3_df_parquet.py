from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import col, desc

# Αρχικοποίηση spark session.
my_spark_session = SparkSession \
    .builder \
    .appName("Q3_df_parquet_input") \
    .getOrCreate() \

# Αρχικοποίηση spark context.   
my_spark_context = my_spark_session.sparkContext

# Ελαχιστοποίηση εξόδων καταγραφής.
my_spark_context.setLogLevel("ERROR")

input_file_path_2024 = "hdfs://hdfs-namenode:9000/user/dikaragiannis/data/parquet/yellow_tripdata_2024"
input_file_path_lookup = "hdfs://hdfs-namenode:9000/user/dikaragiannis/data/parquet/taxi_zone_lookup"
output_dir_path = "hdfs://hdfs-namenode:9000/user/dikaragiannis/project_2025_outputs"

tripdata_df = my_spark_session.read.parquet(input_file_path_2024)
lookup_df = my_spark_session.read.parquet(input_file_path_lookup)

boroughs = ["Manhattan", "Queens", "Brooklyn", "Bronx", "Staten Island"]

# Υπολογισμός πρώτου join.
first_join_df = (
    tripdata_df
        .join(lookup_df, tripdata_df.PULocationID == lookup_df.LocationID)
        .withColumnRenamed("Borough", "Borough_enter")
        .drop("LocationID")
)

# Εμφάνιση execution plan.
print("Below is the execution plan for the first join based on the PULocationID: \n")
first_join_df.explain(True)

# Υπολογισμός δεύτερου join.
second_join_df = (
    first_join_df
        .join(lookup_df, tripdata_df.DOLocationID == lookup_df.LocationID)
        .withColumnRenamed("Borough", "Borough_drop")
        .drop("LocationID")
)

# Εμφάνιση execution plan.
print("Below is the execution plan for the second join based on the DOLocationID: \n")
second_join_df.explain(True)

# Υπολογισμός ζητούμενου dataframe (πλήθος διαδρομών που ξεκίνησαν και κατέληξαν στον ίδιο δήμο).
total_trips_df = (
    second_join_df
        .filter((col("Borough_enter") == col("Borough_drop")) & (col("Borough_enter").isin(*boroughs)))
        .groupBy("Borough_enter")
        .count()
        .withColumnRenamed("Borough_enter", "Borough")
        .withColumnRenamed("count", "TotalTrips")
        .orderBy(desc("TotalTrips"))
)

total_trips_df.show(truncate = False)

# Το output θα είναι ακριβώς το ίδιο αποθηκεύεται σε ξεχωριστό αρχείο απλά για λόγους "καθαρότητας" και οργάνωσης.
total_trips_df.coalesce(1).write.mode("overwrite").option("header", True).csv(f"{output_dir_path}/Q3_df_parquetInput_output")

my_spark_session.stop()




