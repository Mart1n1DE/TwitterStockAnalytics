from kafka import KafkaProducer
from datetime import datetime
import tweepy
import sys 
import re
import os

KAFKA_BROKER = 'localhost:9092'

bearer_token = os.environ["Twitter_Bearer_Token"]
TICKER_SYMBOLS = ['GOOGL']#,'MSFT','TSLA','AMZN','META','NVDA','NFLX','PYPL']
KAFKA_TOPIC = 'tweets'

def removerules ():
	rules = tweepy.StreamingClient.get_rules(stream)
	try:
		for id in rules[0]:
			stream.delete_rules(id.id)
	except:
		print("no rules")
			
def addrules (rules):
	for rule in rules:
		stream.add_rules(tweepy.StreamRule(value = rule))

class Streamer(tweepy.StreamingClient):
    
    def on_tweet(self,status):
        tweet = status.text
        #print(status.text)
        tweet = re.sub(r'RT\s@w*:\s','', tweet)
        tweet = re.sub(r'https?.*','', tweet)
        global producer 
        producer.send(KAFKA_TOPIC,bytes(tweet,encoding = 'utf-8'))
        d = datetime.now()
        print(f'[{d.hour}:{d.minute}.{d.second}] sending tweet')

stream = Streamer(bearer_token = bearer_token, wait_on_rate_limit = True)
removerules()
addrules(TICKER_SYMBOLS)
print(tweepy.StreamingClient.get_rules(stream))

try:
    producer = KafkaProducer(bootstrap_servers = KAFKA_BROKER)
except Exception as e:
    print(f'Error Connection to Kafka: {e}')
    sys.exit(1)
print('reached stream filter')
stream.filter()
