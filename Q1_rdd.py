from pyspark.sql import SparkSession

# Αρχικοποίηση spark session.
my_spark = SparkSession \
    .builder \
    .appName("Q1_rdd") \
    .getOrCreate() \
    .sparkContext 

# Ελαχιστοποίηση εξόδων καταγραφής.
my_spark.setLogLevel("ERROR")

input_file_path = "hdfs://hdfs-namenode:9000/data/yellow_tripdata_2015.csv"
output_dir_path = "hdfs://hdfs-namenode:9000/user/dikaragiannis/project_2025_outputs"

tripdata_rdd = my_spark.textFile(input_file_path)

rdd_header = tripdata_rdd.first()

# Φιλτράρισμα του dataset έτσι ΄ώστε να μην περιέχει τα ονόματα των στηλών.
tripdata_rdd = tripdata_rdd.filter(lambda line : line != rdd_header)

coordinates_by_hour = (
    tripdata_rdd
        .map(lambda x : x.split(",")) # Χωρισμός του df σε λίστες (κάθε γραμμή και μια λίστα).
        .filter(lambda col : col[1][0] != 0 and col[1][1] != 0) # Φιλτράρισμα longitude != 0 and latitude != 0.
        # Tαξινόμηση των δεδομένων στην μορφή (hour, (longitude, latitude, 1)) το ένα αποτελεί μετρητή εγγραφών για τον υπολογισμό μετά του mean.
        .map(lambda col : (col[1][11:13], (float(col[5]), float(col[6]), 1)))
        .reduceByKey(lambda x,y : (x[0] + y[0], x[1] + y[1], x[2] + y[2])) # Υπολογισμός αθροισμάτων (longitude, latitude, counter).
        .mapValues(lambda col : (col[0] / col[2], col[1] / col[2])) # Υπολογισμός mean longitude and mean latitude.
        .sortByKey() # Ταξινόμηση σε αύξουσα σειρά.
        )

# Δημιουργία rdd για το αποτέλεσμα.
result_header = my_spark.parallelize(["HourOfDay\t\tLongitude\t\tLatitude"])
result_rdd = coordinates_by_hour.map(lambda v : f"{v[0]}\t\t{v[1][0]}\t\t{v[1][1]}")
final_rdd = result_header.union(result_rdd)

# Αποθήκευση της εξόδου σε ένα αρχείο.
final_rdd.coalesce(1).saveAsTextFile(f"{output_dir_path}/Q1_rdd_output")

my_spark.stop()


