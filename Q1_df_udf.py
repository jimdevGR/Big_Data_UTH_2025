from pyspark.sql import SparkSession
from pyspark.sql.functions import col, udf, sum, count, hour, to_timestamp
from pyspark.sql.types import DoubleType

# Αρχικοποίηση spark session.
my_spark_session = SparkSession \
    .builder \
    .appName("Q1_dataframe_udf") \
    .getOrCreate() \

# Αρχικοποίηση spark context.   
my_sc = my_spark_session.sparkContext

# Ελαχιστοποίηση εξόδων καταγραφής.
my_sc.setLogLevel("ERROR")

input_file_path = "hdfs://hdfs-namenode:9000/data/yellow_tripdata_2015.csv"
output_dir_path = "hdfs://hdfs-namenode:9000/user/dikaragiannis/project_2025_outputs"

tripdata_df = my_spark_session.read.option("header", True).option("inferSchema", True).csv(input_file_path)

# Συνάρτηση για τον υπολογισμό του μέσου όρου.
def calculate_mean(sum_, count_):
    return sum_ / count_

# Καταχώρηση του udf
calculate_mean_udf = udf(calculate_mean, DoubleType())

sum_coordinates_df = (
    tripdata_df 
        .withColumn("HourofDay", hour(to_timestamp(col("tpep_pickup_datetime")))) # Εξαγωγή της ώρας.
        .filter((col("pickup_longitude") != 0) & (col("pickup_latitude") != 0)) # Έλεγχος αν οι συντεταγμένες είναι μηδέν.
        .groupBy("HourofDay")
        .agg(
             sum(col("pickup_longitude")).alias("longitude_sum"), # Υπολογισμός αθροισμάτων.
             sum(col("pickup_latitude")).alias("latitude_sum"),
             count(col("pickup_latitude")).alias("records_count") # Υπολογισμός πλήθους εγγραφών σε κάθε ώρα.
            )
    )

avg_coordinates_df = (
    sum_coordinates_df
        .withColumn("Longitude", calculate_mean_udf(col("longitude_sum"), col("records_count"))) # Υπολογισμός μέσων ΄΄ορων.
        .withColumn("Latitude", calculate_mean_udf(col("latitude_sum"), col("records_count")))
        .select("HourofDay", "Longitude", "Latitude")
        .orderBy("HourofDay")
    )

avg_coordinates_df.show(24, truncate = False) # Εμφάνιση του DataFrame στο τερματικό.

# Αποθήκευση της εξόδου σε ένα αρχείο.
avg_coordinates_df.coalesce(1).write.mode("overwrite").option("header", True).csv(f"{output_dir_path}/Q1_dataframe_udf_output")

my_spark_session.stop()
my_sc.stop()
