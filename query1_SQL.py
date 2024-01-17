from pyspark.sql.window import Window
from pyspark.sql import SparkSession
from pyspark.sql.functions import col,month,year,count,to_timestamp,row_number,desc
from pyspark.sql.types import DateType, IntegerType, DoubleType

# Create a Spark session
spark = SparkSession.builder.appName("Query1").getOrCreate()

# Define the file path of your main dataset CSV file
main_dataset_path = "hdfs://okeanos-master:54310/user/user/ALLDATA/Crime_Data_from_2010_to_2019.csv"
main_dataset_path_sec = "hdfs://okeanos-master:54310/user/user/ALLDATA/Crime_Data_from_2020_to_Present.csv"

column_name = ("DATE OCC")

# Creation of Dataframes (wiht the desired schemas) from CSVs
main_dataset = spark.read.csv(main_dataset_path, header=True).select(to_timestamp(col(column_name), 'MM/dd/yyyy hh:mm:ss a').alias('DATE_OCC'))
main_dataset_sec = spark.read.csv(main_dataset_path_sec, header=True).select(to_timestamp(col(column_name), 'MM/dd/yyyy hh:mm:ss a').alias('DATE_OCC'))
main_dataset_df = main_dataset.union(main_dataset_sec)

main_dataset_df.createOrReplaceTempView("mainDF")

query = "SELECT EXTRACT(YEAR FROM DATE_OCC) AS year, EXTRACT(MONTH FROM DATE_OCC) AS month FROM mainDF"
help_df = spark.sql(query)

help_df.registerTempTable("help")

query2 = "select year,month,count(*) as crime_total FROM help group by year,month having count(*) > 2 order by year,month"

help_df2 = spark.sql(query2)
help_df2.registerTempTable("help2")

query3 = "SELECT year,month,crime_total,count \
        FROM (Select year,month,crime_total, ROW_NUMBER() OVER (PARTITION BY year ORDER BY crime_total DESC) as rank, ROW_NUMBER() OVER (PARTITION BY year ORDER BY crime_total DESC) % 4 as count \
        FROM help2) ranked \
        WHERE rank <=3"

spark.sql(query3).show(50)
