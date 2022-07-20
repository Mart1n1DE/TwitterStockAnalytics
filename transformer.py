from pyspark import SparkContext                                                                                        
from pyspark.sql import SparkSession                                                                                    
from pyspark.streaming import StreamingContext                                                                          
from pyspark.streaming.kafka import KafkaUtils        
from textblob import TextBlob                                                                  

def handle_rdd(rdd):                                                                                                    
    if not rdd.isEmpty():                                                                                               
        global ss                                                                                                       
        df = ss.createDataFrame(rdd, schema=['text', 'sentiment', 'company'])                                                
        df.show()                                                                                                       
        df.write.saveAsTable(name='default.tweets', format='hive', mode='append')                                       
                        

TICKER_SYMBOLS = ['TSLA','MSFT','GOOG','AMZN','META','NVDA','NFLX','PYPL']                                                                                           
sc = SparkContext(appName="TwitterStreamer")                                                                                     
ssc = StreamingContext(sc, 5)                                                                                           
                                                                                                                        
ss = SparkSession.builder \                                                                                             
        .appName("TwitterStreamer") \                                                                                            
        .config("spark.sql.warehouse.dir", "/user/hive/warehouse") \                                                    
        .config("hive.metastore.uris", "thrift://localhost:9083") \                                                     
        .enableHiveSupport() \                                                                                          
        .getOrCreate()                                                                                                  
                                                                                                                        
ss.sparkContext.setLogLevel('WARN')                                                                                     
                                                                                                                        
ks = KafkaUtils.createDirectStream(ssc, ['tweets'], {'metadata.broker.list': 'localhost:9092'})                       
                                                                                                                        
lines = ks.map(lambda x: x[1])                                                                                          

for ticker in TICKER_SYMBOLS:
    transform = lines.filter(lambda tweet: ticker in tweet)
    transform = transform.map(lambda tweet: (tweet[0],TextBlob(tweet[0]).sentiment.polarity,ticker))
    transform.foreachRDD(handle_rdd)

                                                                                                                                                        
transform.foreachRDD(handle_rdd)                                                                                        
                                                                                                                        
ssc.start()                                                                                                             
ssc.awaitTermination()