"""
Δηλώνοντας τα configurations με χρήση της .config() κατά την αρχικοποίηση του spark session παρακάμπτουμε το 
spark-defaults.conf μόνο για αυτήν την εργασία επομένως αυτός ο κώδικας εφαρμόζει οριζόντια κλιμάκωση.
"""

from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import col, desc, format_number, sum

# Αρχικοποίηση spark session.
my_spark_session = SparkSession \
    .builder \
    .config("spark.executor.instances", "8") \
    .config("spark.executor.cores", "1") \
    .config("spark.executor.memory", "2g") \
    .appName("Q6_df_horizontal_scaling") \
    .getOrCreate() 

# Αρχικοποίηση spark context.   
my_spark_context = my_spark_session.sparkContext

# Ελαχιστοποίηση εξόδων καταγραφής.
my_spark_context.setLogLevel("ERROR")

input_file_path_2024 =  "hdfs://hdfs-namenode:9000/user/dikaragiannis/data/parquet/yellow_tripdata_2024"
input_file_path_lookup = "hdfs://hdfs-namenode:9000/user/dikaragiannis/data/parquet/taxi_zone_lookup"
output_dir_path = "hdfs://hdfs-namenode:9000/user/dikaragiannis/project_2025_outputs"

boroughs = ["Manhattan", "Queens", "Brooklyn", "Bronx", "Staten Island"]

tripdata_df = my_spark_session.read.parquet(input_file_path_2024) \
.select(col("PULocationID"), col("fare_amount"), col("tip_amount"), col("tolls_amount"), col("extra"), 
        col("mta_tax"), col("congestion_surcharge"), col("airport_fee"), col("total_amount"))

lookup_df = my_spark_session.read.parquet(input_file_path_lookup).select(col("Borough"), col("LocationID"))

# Υπολογισμός των συνολικών εσόδων από διαδρομές ταξί κάθε δήμου.
finances_df = (
    tripdata_df
        .join(lookup_df, tripdata_df.PULocationID == lookup_df.LocationID)
        .drop("PULocationID")
        .drop("LocationID")
        .filter((col("Borough").isin(*boroughs)))
        .groupBy("Borough")
        .agg(
             sum("fare_amount").alias("fare"),
             sum("tip_amount").alias("tip"),
             sum("tolls_amount").alias("tolls"),
             sum("extra").alias("extra_"),
             sum("mta_tax").alias("mta_tax_"),
             sum("congestion_surcharge").alias("congestion"),
             sum("airport_fee").alias("airport"),
             sum("total_amount").alias("total")
            )
        .orderBy(desc("total"))
        .select(
                "Borough",
                format_number("fare", 2).alias("Fare ($)"),  # Χρήση format_number για να μην εμφανίζονται στο τερματικό οι αριθμοί σε επιστημονική μορφή.
                format_number("tip", 2).alias("Tips ($)"),
                format_number("tolls", 2).alias("Tolls ($)"),
                format_number("extra_", 2).alias("Extras ($)"),
                format_number("mta_tax_", 2).alias("MTA Tax ($)"),
                format_number("congestion", 2).alias("Congestion ($)"),
                format_number("airport", 2).alias("Airport Fee ($)"),
                format_number("total", 2).alias("Total Revenue ($)")
               )
)

finances_df.show(truncate = False)

# Το output θα είναι το ίδιο και στα 2 scaling.
finances_df.coalesce(1).write.mode("overwrite").option("header", True).csv(f"{output_dir_path}/Q6_df_horizontal_scaling_output")

my_spark_session.stop()