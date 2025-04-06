from pyspark.sql import SparkSession
from pyspark.sql.functions import col,month,year,count,desc,when
from pyspark.sql.types import IntegerType
import time

# Create a Spark session
spark = SparkSession.builder.appName("Query2_Dataframe").getOrCreate()

# Define the file path of your main dataset CSV file
main_dataset_path = "hdfs://okeanos-master:54310/user/user/ALLDATA/Crime_Data_from_2010_to_2019.csv"
main_dataset_path_sec = "hdfs://okeanos-master:54310/user/user/ALLDATA/Crime_Data_from_2020_to_Present.csv"

main_dataset = spark.read.csv(main_dataset_path, header=True).select("TIME OCC","Premis Desc")
main_dataset_sec = spark.read.csv(main_dataset_path_sec, header=True).select("TIME OCC","Premis Desc")
main_dataset_df = main_dataset.union(main_dataset_sec)

def categorize_time(ts_column):
    return when((ts_column > '500') & (ts_column < '1159'), 'Morning')\
        .when((ts_column > '1200') & (ts_column < '1659'), 'Afternoon')\
        .when((ts_column > '1700') & (ts_column < '2059'), 'Night')\
        .otherwise('Late_night')

#Start time measurment
start_time = time.time()

filtered_df = main_dataset_df.filter(main_dataset_df["Premis Desc"]=="STREET")
filtered_df = filtered_df.withColumn("TIME OCC",col("TIME OCC").cast(IntegerType()))


filtered_df = filtered_df.withColumn('Time_Category', categorize_time(col('TIME OCC')))

result_df = (
    filtered_df.groupBy('Time_Category')
    .agg(count('*').alias('Occurrences'))
    .orderBy(desc('Occurrences'))
)

end_time = time.time()
result_df.show()
print ("Elapsed time only for the query calculation: ",end_time - start_time ,"seconds")