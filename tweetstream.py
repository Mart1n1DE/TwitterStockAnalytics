from kafka import KafkaProducer
from datetime import datetime
import tweepy
import sys 
import re
import os

KAFKA_BROKER = 'localhost:9092'
bearer_token = os.environ["Twitter_Bearer_Token"]
TICKER_SYMBOLS = ['GOOGL','MSFT','TSLA','AMZN','META','NVDA','NFLX','PYPL']
KAFKA_TOPIC = 'tweets'

#function that removes all pre-existing filters for topics in tweepy StreamingClient object
def removerules ():
	rules = tweepy.StreamingClient.get_rules(stream)
	try:
		for id in rules[0]:
			stream.delete_rules(id.id)
	except:
		print("no rules")

#function that adds filters for topics
#param: rules 
#type: list of Strings
def addrules (rules):
	for rule in rules:
		stream.add_rules(tweepy.StreamRule(value = rule))

#define Streamer subclass of tweepy StreamingClient
class Streamer(tweepy.StreamingClient):
#function that upon receiving tweet from StreamingClient 
#uses regex to clean up the tweet's text and send it to a Kafka Producer
    def on_tweet(self,status):
        tweet = status.text
        #print(status.text)
        tweet = re.sub(r'RT\s@w*:\s','', tweet)
        tweet = re.sub(r'https?.*','', tweet)
        global producer 
        producer.send(KAFKA_TOPIC,bytes(tweet,encoding = 'utf-8'))
        #print time upon sending tweet
        d = datetime.now()
        print(f'[{d.hour}:{d.minute}.{d.second}] sending tweet')
#initialize Streamer object 
stream = Streamer(bearer_token = bearer_token, wait_on_rate_limit = True)
#remove pre-existing filters to StreamingClient 
removerules()
#add desired filters to StreamingClient
addrules(TICKER_SYMBOLS)
print(tweepy.StreamingClient.get_rules(stream))
#initialize Kafka Producer
try:
    producer = KafkaProducer(bootstrap_servers = KAFKA_BROKER)
except Exception as e:
    print(f'Error Connection to Kafka: {e}')
    sys.exit(1)
#start Twitter Stream
stream.filter()
