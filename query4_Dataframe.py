from pyspark.sql import SparkSession
from pyspark.sql.functions import col,month,year,avg,count,to_timestamp,row_number,desc,when
from pyspark.sql.types import DateType, IntegerType, DoubleType
import time
#import geopy.distance
from math import radians, sin, cos, sqrt, atan2

# calculate the distance between two points [lat1, long1], [lat2, long2] in km
#def get_distance(lat1, long1, lat2, long2):
#  return geopy.distance.geodesic((lat1, long1), (lat2, long2)).km

# other ways to calculate distance
# calculating haversine distance (from coordinates to km)
def get_distance_2(lat1, long1, lat2, long2):
    # Convert latitude and longitude from degrees to radians
    lat1, long1, lat2, long2 = map(radians, [lat1, long1, lat2, long2])
    # Haversine formula
    dlat = lat2 - lat1
    dlon = long2 - long1
    a = sin(dlat / 2) ** 2 + cos(lat1) * cos(lat2) * sin(dlon / 2) ** 2
    c = 2 * atan2(sqrt(a), sqrt(1 - a))
    # Radius of the Earth in kilometers (mean value) : radius = 6371.0
    # Calculate and return the distance
    return 6371.0 * c

# Create a Spark session
spark = SparkSession.builder.appName("Query4_First_Dataframe").getOrCreate()

# File paths of the CSV files
main_dataset_path = "hdfs://okeanos-master:54310/user/user/ALLDATA/Crime_Data_from_2010_to_2019.csv"
main_dataset_path_sec = "hdfs://okeanos-master:54310/user/user/ALLDATA/Crime_Data_from_2020_to_Present.csv"
police_station_path = "hdfs://okeanos-master:54310/user/user/ALLDATA/LAPD_Police_Stations.csv"

main_dataset = spark.read.csv(main_dataset_path, header=True).select(to_timestamp(col("DATE OCC"), 'MM/dd/yyyy hh:mm:ss a').alias('DATE_OCC'),"AREA ","Weapon Used Cd","LAT","LON")
main_dataset_sec = spark.read.csv(main_dataset_path_sec, header=True).select(to_timestamp(col("DATE OCC"), 'MM/dd/yyyy hh:mm:ss a').alias('DATE_OCC'),"AREA","Weapon Used Cd","LAT","LON")

# Renaming the column in the first dataset to match the second dataset
main_dataset = main_dataset.withColumnRenamed("AREA ", "AREA")

main_dataset_df = main_dataset.union(main_dataset_sec)
police_station_df = spark.read.csv(police_station_path, header=True).select("X","Y","DIVISION","PREC")

# Casting coordinates to DoubleType
main_dataset_df = main_dataset_df.withColumn("LAT",col("LAT").cast(DoubleType()))
main_dataset_df = main_dataset_df.withColumn("LON",col("LON").cast(DoubleType()))
police_station_df = police_station_df.withColumn("X",col("X").cast(DoubleType()))
police_station_df = police_station_df.withColumn("Y",col("Y").cast(DoubleType()))

# Getting Year column from the "DATE_OCC" column
main_dataset_df = main_dataset_df.withColumn("Year", year("DATE_OCC"))

# Filter out Null Island records
main_dataset_df = main_dataset_df.filter((col("LAT") != 0) & (col("LON") != 0))

# Only incidents with firearms (Weapon Used Cd of the form '1xx')
firearm_crimes = main_dataset_df.filter(col("Weapon Used Cd").rlike("[1].*"))

# Registering our own User Defined Function (UDF) to calculate distance
#get_distance_udf = spark.udf.register("get_distance", get_distance)
get_distance_2_udf = spark.udf.register("get_distance_2", get_distance_2)

# Joining firearm_crimes with police_station correctly ("AREA" from main_dataset is the equivalent to "PREC" from police_station)
combined_dataset = firearm_crimes.join(police_station_df, firearm_crimes["AREA"] == police_station_df["PREC"], "inner")

# Calculating distance for each crime from police station
#result = combined_dataset.withColumn("distance", get_distance_udf(col("LAT"), col("LON"), col("X"), col("Y")))
result = combined_dataset.withColumn("distance", get_distance_2_udf(col("LAT"), col("LON"), col("X"), col("Y")))

# Average distance and number of incidents per year
result_a = result.groupBy("Year").agg(avg("distance").alias("average_distance"), count("*").alias("number_of_incidents"))
result_a = result_a.orderBy("Year")

# Display the results
result_a.show()

# Average distance and number of incidents per police station
result_b = result.groupBy("DIVISION").agg(avg("distance").alias("average_distance"), count("*").alias("number_of_incidents"))
result_b = result_b.orderBy(desc("number_of_incidents"))

# Display the results
result_b.show()