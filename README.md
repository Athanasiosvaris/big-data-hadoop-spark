# ntua-advanced-databases
Term project for the course 'Advanced Databases' during 9th semester at NTUA

# Creators 
1. Athanasios Varis
2. Georgios Kalaras

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
