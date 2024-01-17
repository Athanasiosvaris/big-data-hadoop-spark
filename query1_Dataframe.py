from pyspark.sql.window import Window
from pyspark.sql import SparkSession
from pyspark.sql.functions import col,month,year,count,to_timestamp,row_number,desc
from pyspark.sql.types import DateType, IntegerType, DoubleType

# Create a Spark sessio
spark = SparkSession.builder.appName("Query1").getOrCreate()

# Define the file path of your main dataset CSV file
main_dataset_path = "hdfs://okeanos-master:54310/user/user/ALLDATA/Crime_Data_from_2010_to_2019.csv"
main_dataset_path_sec = "hdfs://okeanos-master:54310/user/user/ALLDATA/Crime_Data_from_2020_to_Present.csv"

column_name = ("DATE OCC")

# Creation of Dataframes (wiht the desired schemas) from CSVs
main_dataset = spark.read.csv(main_dataset_path, header=True).select(to_timestamp(col(column_name), 'MM/dd/yyyy hh:mm:ss a').alias('DATE_OCC'))
main_dataset_sec = spark.read.csv(main_dataset_path_sec, header=True).select(to_timestamp(col(column_name), 'MM/dd/yyyy hh:mm:ss a').alias('DATE_OCC'))
main_dataset_df = main_dataset.union(main_dataset_sec)
#main_dataset_df.show()
#main_dataset_df.printSchema

# New DF with year and month columns from the "DATE_OCC" column
grouped_df = main_dataset_df.withColumn("year", year("DATE_OCC"))\
                            .withColumn("month", month("DATE_OCC"))
#grouped_df.show()
#grouped_df.printSchema()

# Group by year and month, and count occurrences
result_df = grouped_df.groupBy("year", "month").agg(count("*").alias("crime_total"))

#result_df.show()
#result_df.printSchema()

# Define a window specification for each year, ordered by the count in descending order
window_spec = Window.partitionBy("year").orderBy(desc("crime_total"))

# Rank the rows within each year based on the count row_number => arithmei kathe row tou window spec
result_df = result_df.withColumn("#", row_number().over(window_spec))

#Keep only top 3
result_df = result_df.filter(col("#") <= 3)

result_df.show(50)
#result_df.printSchema()
