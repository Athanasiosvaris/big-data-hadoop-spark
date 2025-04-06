from pyspark import SparkContext
from pyspark.sql import SparkSession
import time

# Create a Spark session
spark = SparkSession.builder.appName("Query2_RDD").getOrCreate()

# Define the file path of your main dataset CSV file
main_dataset_path = "hdfs://okeanos-master:54310/user/user/ALLDATA/Crime_Data_from_2010_to_2019.csv"
main_dataset_path_sec = "hdfs://okeanos-master:54310/user/user/ALLDATA/Crime_Data_from_2020_to_Present.csv"

# Read CSV files and select required columns
main_dataset_rdd = spark.read.csv(main_dataset_path, header=True).select("TIME OCC", "Premis Desc").rdd
main_dataset_sec_rdd = spark.read.csv(main_dataset_path_sec, header=True).select("TIME OCC", "Premis Desc").rdd
main_dataset_rdd = main_dataset_rdd.union(main_dataset_sec_rdd)

# Define function to categorize time
def categorize_time(ts_column):
    return (
        "Morning" if 500 < ts_column < 1159 else
        "Afternoon" if 1200 < ts_column < 1659 else
        "Night" if 1700 < ts_column < 2059 else
         "Late_night"
    )

#Start time
start_time = time.time()

filtered_rdd = main_dataset_rdd.filter(lambda row: row["Premis Desc"] == "STREET")
filtered_rdd = filtered_rdd.map(lambda row: (int(row["TIME OCC"]), row["Premis Desc"]))
filtered_rdd = filtered_rdd.map(lambda row: (row[0], row[1], categorize_time(row[0])))
# Count occurrences for each time category
result_rdd = filtered_rdd.map(lambda row: (row[2], 1)).reduceByKey(lambda x, y: x + y)
result_rdd = result_rdd.sortBy(lambda x: x[1], ascending=False)

end_time = time.time()

# Display the final result
print("Result RDD:")
print(result_rdd.collect())
print("Elapsed time only for the query calculation: ",end_time - start_time ,"seconds")