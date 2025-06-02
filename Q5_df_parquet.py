from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import col, desc

# Αρχικοποίηση spark session.
my_spark_session = SparkSession \
    .builder \
    .appName("Q5_df_parquet_input") \
    .getOrCreate() \

# Αρχικοποίηση spark context.   
my_spark_context = my_spark_session.sparkContext

# Ελαχιστοποίηση εξόδων καταγραφής.
my_spark_context.setLogLevel("ERROR")

input_file_path_2024 = "hdfs://hdfs-namenode:9000/user/dikaragiannis/data/parquet/yellow_tripdata_2024"
input_file_path_lookup = "hdfs://hdfs-namenode:9000/user/dikaragiannis/data/parquet/taxi_zone_lookup"
output_dir_path = "hdfs://hdfs-namenode:9000/user/dikaragiannis/project_2025_outputs"

tripdata_df = my_spark_session.read.parquet(input_file_path_2024).select(col("PULocationID"), col("DOLocationID"))
lookup_df = my_spark_session.read.parquet(input_file_path_lookup).select(col("Zone"), col("LocationID"))

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

zone_pairs_df.coalesce(1).write.mode("overwrite").option("header", True).csv(f"{output_dir_path}/Q5_df_parquetInput_output")
                                                                              
my_spark_session.stop()