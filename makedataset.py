from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from pyspark.sql.types import DateType, IntegerType, DoubleType

# Create a Spark session
spark = SparkSession.builder.appName("LosAngelesCrime").getOrCreate()

# Define the file path of your main dataset CSV file
main_dataset_path = "hdfs://okeanos-master:54310/user/user/ALLDATA/Crime_Data_from_2010_to_2019.csv"
main_dataset_path_sec = "hdfs://okeanos-master:54310/user/user/ALLDATA/Crime_Data_from_2020_to_Present.csv"

# Define the schema for your DataFrame
main_dataset_schema = ("DR_NO INT, Date_Rptd STRING, DATE_OCC STRING, TIME_OCC STRING, AREA INT, "
                       "AREA_NAME STRING, RPTDISTNO INT, PART12 INT, Crm_Cd INT, CrmCd_Desc STRING, Mocodes INT, "
                       "Vict_Age INT, Vict_Sex STRING, Vict_Descent STRING, Premis_Cd INT, "
                       "Premis_Desc STRING, Weapon_Used_Cd INT, Weapon_Desc STRING, "
                       "Status STRING, Status_Desc STRING, Crm_Cd1 INT, Crm_Cd2 INT, Crm_Cd3 INT, Crm_Cd4 INT, "
                       "LOCATION STRING, Cross_Street STRING, LAT FLOAT, LON FLOAT"
                       )

# Read the CSV file with the defined schema
main_dataset = spark.read.csv(main_dataset_path, header=True, sep=',', schema=main_dataset_schema)
main_dataset_sec = spark.read.csv(main_dataset_path_sec, header=True, sep=',', schema=main_dataset_schema)

main_dataset_df = main_dataset.union(main_dataset_sec)

# Convert columns to the specified types
main_dataset_df = main_dataset_df.withColumn("Date_Rptd", col("Date_Rptd").cast(DateType()))
main_dataset_df = main_dataset_df.withColumn("DATE_OCC", col("DATE_OCC").cast(DateType()))
main_dataset_df = main_dataset_df.withColumn("Vict_Age", col("Vict_Age").cast(IntegerType()))
main_dataset_df = main_dataset_df.withColumn("LAT", col("LAT").cast(DoubleType()))
main_dataset_df = main_dataset_df.withColumn("LON", col("LON").cast(DoubleType()))

# Show the total number of rows and data types of each column
main_dataset_df.printSchema()
print("Total number of rows: ", main_dataset_df.count())
