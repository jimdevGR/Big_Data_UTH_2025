from pyspark.sql import SparkSession

# Αρχικοποίηση spark session.
my_spark_session = SparkSession \
    .builder \
    .appName("join_inspect_change") \
    .getOrCreate() \

# Αρχικοποίηση spark context.   
my_spark_context = my_spark_session.sparkContext

# Ελαχιστοποίηση εξόδων καταγραφής.
my_spark_context.setLogLevel("ERROR")

input_file_path_2024 = "hdfs://hdfs-namenode:9000/user/dikaragiannis/data/parquet/yellow_tripdata_2024"
input_file_path_lookup = "hdfs://hdfs-namenode:9000/user/dikaragiannis/data/parquet/taxi_zone_lookup"

tripdata_df = my_spark_session.read.parquet(input_file_path_2024)

tripdata_df.createOrReplaceTempView("tripdata_table")

# Θέτοντας την παρακάτω μεταβλητή σε -1 αναγκάζουμε τον spark να επιλέξει διαφορετική 
# στρατηγική συννένωσης από την broadcast hash join. Θα τρέξουμε μια φορά τον κώδικα χωρίς να πειράξουμε την 
# μεταβλητή (η παρακάτω γραμμή θα είναι σχόλιο) και μια φορά θέτοντας την -1.

my_spark_session.conf.set("spark.sql.autoBroadcastJoinThreshold", "-1")

# Υλοποίηση δοκιμαστικού ερωτήματος συννένωσης.
my_query = f"""
            SELECT trips.*, lookup.*
            FROM tripdata_table AS trips
            JOIN (
                  SELECT *
                  FROM parquet.`{input_file_path_lookup}`
                  LIMIT 50
                 ) AS lookup
            ON trips.PULocationID == lookup.LocationID       
"""

# Εκτέλεση του ερωτήματος.
joined_df = my_spark_session.sql(my_query)

print("Below is the physical plan for this join: \n")

# Εμφάνιση πλάνου εκτέλεσης (μόνο το physical).
joined_df.explain()

print(f"Total number of rows of the joined dataframe: {joined_df.count()}")

my_spark_session.stop()





