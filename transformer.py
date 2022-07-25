import pyspark
from pyspark import SparkContext                                                                                        
from pyspark.sql import SparkSession                                                                                    
from pyspark.streaming import StreamingContext                                                                           
from textblob import TextBlob                                                                  

def handle_Dataframe(df,batchid):     
	df = df.toDF("text") \
		.rdd
	if not df.isEmpty():
		for ticker in TICKER_SYMBOLS: 
			transform = df.filter(lambda text: ticker in text[0])
			if not transform.isEmpty():
				transform = transform.map(lambda text: (text[0],TextBlob(text[0]).sentiment.polarity,ticker))          
				write_dataframe = ss.createDataFrame(transform, schema = ['text','sentiment','company'])
				write_dataframe.show()
				write_dataframe.write.saveAsTable(name='default.tweets', format = 'hive', mode = 'append')
                        
TICKER_SYMBOLS = ['GOOGL']#,'MSFT','TSLA','AMZN','META','NVDA','NFLX','PYPL']                                                                                           
KAFKA_TOPIC = 'tweets'
KAFKA_SERVER = 'localhost:9092'
                                                                             
ss = SparkSession \
	.builder \
	.appName("TwitterStreamer") \
	.config("spark.sql.warehouse.dir", "/user/hive/warehouse") \
	.config("hive.metastore.uris", "thrift://localhost:9083") \
	.enableHiveSupport() \
	.getOrCreate()        
                                                                                                                 
ss.sparkContext.setLogLevel('WARN')                                                                                     
                                                                                                                                                  
kafka_connection = ss \
	.readStream \
	.format("kafka") \
	.option("kafka.bootstrap.servers",KAFKA_SERVER) \
	.option("subscribe",KAFKA_TOPIC) \
	.load() \

kafka_dataframe = kafka_connection.selectExpr("CAST(value as STRING)") \
	.writeStream \
	.foreachBatch(handle_Dataframe) \
	.outputMode("append") \
	.start().awaitTermination()
                                                                                                                                                                                                          
