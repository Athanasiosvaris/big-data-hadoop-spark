# big-data-hadoop-spark
 Semester project for "Advanced Databases" course. Extracting valuable analytics from a large dataset in a distributed cloud environment using Apache Hadoop and Apache Spark.

# Goal
This project exists because :<br/>
a) It was mandatory in order to pass the course :laughing: and <br/>
b) To showcase the capabilities of modern big data frameworks.Using Hadoop for storage and Spark for fast analytics,all in a cloud-friendly setup this project will help users understand how to build scalable data pipelines, process massive datasets efficiently, and extract meaningful insights that traditional tools can't handle.

# Install
<b>1.</b> Install Apache Spark and Hadoop on your (virtual) machines. <br/>To fully leverage their distributed processing capabilities, it is recommended to have at least two nodes — one configured as the master/worker node and the other as worker node.You can find a sample guide <a href="https://colab.research.google.com/drive/1pjf3Q6T-Ak2gXzbgoPpvMdfOHd1GqHZG?usp=sharing" target="_blank">here</a> for setting up your virtual machines.

<p>If you choose not to follow the guide and you want to use your desired cloud environment, you have to follow the following steps:</p> 
<ol type="a">
  <li>Create 2 virtual machines with the following characteristics:
    <ul>
      <li>Ubuntu Server LTS 22.04 OS</li>
      <li>4 CPUs</li>
      <li>8GB RAM</li>
      <li>30GB disk capacity</li>
    </ul>
  </li>
  <li>Create a private network through which the virtual machines can communicate with one another.</li>
  <li>Install the latest Java version in both virtual machines.</li>
  <li>
    Install the latest version of Apache 
    <a href="https://hadoop.apache.org/releases.html" target="_blank">Hadoop</a> and 
    Apache <a href="https://spark.apache.org/downloads.html" target="_blank">Spark</a> 
    using their official sites.
  </li>
</ol>

          
<b>2.</b> Download in both machines the datasets (.csv) upon which we will be working: <br/>
<ol type = "b"> 
<ul>
   <li>Crime Data from 2010 to 2019  <a href="https://catalog.data.gov/dataset/crime-data-from-2010-to-2019" target="_blank">link</a></li>
   <li>Crime Data from 2020 to present <a href = "https://catalog.data.gov/dataset/crime-data-from-2020-to-present" target="_blank">link</a></li>
   <li>LAPD Polic Stations  <a href = "https://geohub.lacity.org/datasets/lahub::lapd-police-stations/explore" target="_blank">link</a></li>
   <li>Median Household Income by Zip Code Los Angeles County <a href = "https://www.laalmanac.com/employment/em12c_2015.php" target="_blank">link</a></li>
</ul>
</ol>        
<b>3.</b>Use  <code>hdfs -dfs put</code>  to import the data inside HDFS (Hadoop's distrubuted filesystem).<br/>
<b>4.</b> Run each query and view the results.

# Contact
Submit an issue here on GitHub.

