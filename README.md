# big-data-hadoop-spark
 Semester project for "Advanced Databases" course. Extracting valuable analytics from a large dataset in a distributed cloud environment using Apache Hadoop and Apache Spark.

# Creator
1. Athanasios Varis

# Goal
This project exists because :
a)It was mandatory in order to pass the course :laughing: and 
b)To showcase the capabilities of modern big data frameworks.Using Hadoop for storage and Spark for fast analytics,all in a cloud-friendly setup this project will help users understand how to build scalable data pipelines, process massive datasets efficiently, and extract meaningful insights that traditional tools can't handle.

# Install
<b>1.</b> Install Apache Spark and Hadoop on your machines. To fully leverage their distributed processing capabilities, it is recommended to have at least two nodes — one configured as the master/worker and the other as worker node.You can find a sample guide <a href="https://colab.research.google.com/drive/1pjf3Q6T-Ak2gXzbgoPpvMdfOHd1GqHZG?usp=sharing" target="_blank">here</a> for setting up your virtual machines.

<b>2.</b> If you choose your desired cloud enviroment you have to follow the followinng steps:
          a.Create 2 virtual machines with the following characteristics :
            Ubuntu Server LTS 22.04 OS
            4 CPUs
            8GB RAM
            30GB disk capacity
          b.Create a private network through which the virtual machines can communicate with one another
          c.Install latest Java version in both virtual machines
          d.Install latest version of Apache <a href="https://hadoop.apache.org/releases.html" target="_blank">Hadoop</a>  and Apache <a href="https://spark.apache.org/downloads.html" target="_blank">Spark</a>                 using their oficial sites  
         

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
