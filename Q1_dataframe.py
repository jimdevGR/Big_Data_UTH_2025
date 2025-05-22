from pyspark.sql import SparkSession
from pyspark.sql.functions import to_timestamp, hour, col, avg
from pyspark.sql.types import *

# Αρχικοποίηση spark session.
my_spark_session = SparkSession \
    .builder \
    .appName("Q1_dataframe") \
    .getOrCreate() \

# Αρχικοποίηση spark context.   
my_sc = my_spark_session.sparkContext

# Ελαχιστοποίηση εξόδων καταγραφής.
my_sc.setLogLevel("ERROR")

input_file_path = "hdfs://hdfs-namenode:9000/data/yellow_tripdata_2015.csv"
output_dir_path = "hdfs://hdfs-namenode:9000/user/dikaragiannis/project_2025_outputs"

tripdata_df = my_spark_session.read.option("header", True).option("inferSchema", True).csv(input_file_path)

avg_coordinates_df = (
    tripdata_df 
        .withColumn("HourofDay", hour(to_timestamp(col("tpep_pickup_datetime")))) # Εξαγωγή της ώρας.
        .filter((col("pickup_longitude") != 0) & (col("pickup_latitude") != 0)) # Έλεγχος αν οι συντεταγμένες είναι μηδέν.
        .groupBy("HourofDay")
        .agg(
             avg(col("pickup_longitude")).alias("Longitude"), # Υπολογισμός μέσων όρων.
             avg(col("pickup_latitude")).alias("Latitude")
            )
            .orderBy("HourofDay")
    )

avg_coordinates_df.show(24, truncate = False) # Εμφάνιση του DataFrame στο τερματικό.

# Αποθήκευση της εξόδου σε ένα αρχείο.
avg_coordinates_df.coalesce(1).write.mode("overwrite").option("header", True).csv(f"{output_dir_path}/Q1_dataframe_output")

my_spark_session.stop()
my_sc.stop()