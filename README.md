# Twitter Streaming Sentiment Analysis 
## Description
The purpose of this project was to develop a data pipeline that streams live Twitter data of Fortune 500 companies for sentiment analysis.

<picture>
  <img alt="Visualization of Kappa Architecture" src="http://martinhleung.com/wp-content/uploads/2022/07/architecture-1.png">
</picture>

Created Python scripts to stream Twitter data into Kafka Producer.  <br>
Used PySpark to transform and apply pre-trained machine learning model to Twitter data.  <br>
Aggregated and visualized tweets and sentiment analysis results from Hive table.  <br>
For the process in building this project please visit https://martinhleung.com/portfolios#twitterproject.

##Installation and Usage

For this project, I used a virtual box with Ubuntu 22.04 installed. 
I used Python 3.10.4 along with the libraries found in this repo's requirements.txt.
I installed 
Spark 3.3.0 
Kafka 3.2.0
Hadoop 3.3.3
Hive 2.3.9
Scala 2.12

For the rest of installation and usage please visit https://martinhleung.com/portfolios#twitterproject.




