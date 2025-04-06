from pyspark.sql.window import Window
from pyspark.sql import SparkSession
from pyspark.sql.functions import col,month,year,count,to_timestamp,row_number,desc
from pyspark.sql.types import DateType, IntegerType, DoubleType

# Spark session
spark = SparkSession.builder.appName("Query3_DataFrame").getOrCreate()

# The file paths of the CSV files
main_dataset_path = "hdfs://okeanos-master:54310/user/user/ALLDATA/Crime_Data_from_2010_to_2019.csv"
median_household_path = "hdfs://okeanos-master:54310/user/user/ALLDATA/LA_income_2015.csv"
reverse_geocoding_path = "hdfs://okeanos-master:54310/user/user/ALLDATA/revgecoding.csv"

main_dataset_df = spark.read.csv(main_dataset_path, header=True).select("DATE OCC","Vict Descent","LAT","LON")
median_household_df = spark.read.csv(median_household_path, header=True).select("Zip Code","Estimated Median Income")
rev_geocoding_df = spark.read.csv(reverse_geocoding_path, header=True).select("LAT","LON","ZIPcode" )

# Only data for 2015
main_dataset_2015 = main_dataset_df.filter(col("DATE OCC").like("%2015%"))

# filtering for incorrect Victim Descent
filtered_dataset_df = main_dataset_2015.filter((col("Vict Descent").isNotNull()) & (col("Vict Descent") != "Unknown") & (col("Vict Descent") != 'X'))

# crime data with ZIP codes from reverse geocoding
dataset_with_zip = filtered_dataset_df.join(rev_geocoding_df, ["LAT", "LON"], "inner")

# adding income information for each ZIP code
dataset_with_income = dataset_with_zip.join(median_household_df, dataset_with_zip["ZIPcode"] == median_household_df["Zip Code"], "inner")

# Finding the desired ZIP codes from the ZIP codes under examination
dataset_by_income = dataset_with_income.groupBy("Zip Code", "Estimated Median Income").agg(count("*").alias("Number of Victims"))

# The 3 ZIP codes with the highest income
top_income_zipcodes = dataset_by_income.orderBy(desc("Estimated Median Income")).limit(3).select("Zip Code").collect()
top_income_zipcodes = [row["Zip Code"] for row in top_income_zipcodes]

# The 3 ZIP codes with the lowest income
bottom_income_zipcodes = dataset_by_income.orderBy("Estimated Median Income").limit(3).select("Zip Code").collect()
bottom_income_zipcodes = [row["Zip Code"] for row in bottom_income_zipcodes]

# All 6 ZIP codes
all_income_zipcodes = top_income_zipcodes + bottom_income_zipcodes

# Dataset with only the 6 ZIP codes (the 3 highest and the 3 lowest ZIP codes by income)
combined_dataset = dataset_with_income.filter(col("ZIPcode").isin(all_income_zipcodes))

# number of victims per ethnic group for each ZIP code
result_df = combined_dataset.groupBy("Vict Descent", "ZIPcode").agg(count("*").alias("Number of Victims"))

# results ordered by ZIP code
result_df = result_df.orderBy("ZIPcode", desc("Number of Victims"))

# showing the results
result_df.show(100)