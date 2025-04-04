# big-data-hadoop-spark
 Semester project for "Advanced Databases" course. Extracting valuable analytics from a large dataset in a distributed cloud environment using Apache Hadoop and Apache Spark.

# Creator
1. Athanasios Varis

# Goal
This project exists because :
a)It was mandatory in order to pass the course :laughing: and 
b)To showcase the capabilities of modern big data frameworks.Using Hadoop for storage and Spark for fast analytics,all in a cloud-friendly setup this project will help users understand how to build scalable data pipelines, process massive datasets efficiently, and extract meaningful insights that traditional tools can't handle.

# Install
<b>1.</b> Install Apache Spark and Hadoop on your machines. To fully leverage their distributed processing capabilities, it is recommended to have at least three nodes — one configured as the master and two as worker nodes.You can find a sample guide here for setting up your virtual machines here: <a href="https://colab.research.google.com/drive/1pjf3Q6T-Ak2gXzbgoPpvMdfOHd1GqHZG?usp=sharing" target="_blank"></a> 

# Environment Setup and Execution of the Queries
<b>1.</b> Install Apache Spark and Hadoop using the following <a href="https://colab.research.google.com/drive/1pjf3Q6T-Ak2gXzbgoPpvMdfOHd1GqHZG?usp=sharing" target="_blank">guide</a> 

<b>2.</b> Download locally the required datasets (The .csv files) from :   
 a. Crime Data from 2010 to 2019  <a href="https://catalog.data.gov/dataset/crime-data-from-2010-to-2019" target="_blank">link</a>  
 b. Crime Data from 2020 to present <a href = "https://catalog.data.gov/dataset/crime-data-from-2020-to-present" target="_blank">link</a>  
 c. LAPD Polic Stations  <a href = "https://geohub.lacity.org/datasets/lahub::lapd-police-stations/explore" target="_blank">link</a>  
 d. Median Household Income by Zip Code Los Angeles County <a href = "https://www.laalmanac.com/employment/em12c_2015.php" target="_blank">link</a>  
Once you have all 4 datasets in .csv format locally in your machine you can use the <b>scp</b> command in a command promt enviroment in order to transfer the data in the remote machines.   
(Note! You must have the data in ALL nodes of the cluster)  
<b>3.</b> Import the data inside hdfs with <b>hdfs -dfs put</b> command  
<b>4.</b> Run each query
