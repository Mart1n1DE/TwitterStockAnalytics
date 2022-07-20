from kafka import KafkaProducer
from datetime import datetime
import tweepy
import sys 
import re
import os

KAFKA_BROKER = 'localhost:9092'
TICKER_SYMBOLS = ['TSLA','MSFT','GOOG','AMZN','META','NVDA','NFLX','PYPL']
KAFKA_TOPIC = 'tweets'

class Streamer(tweepy.StreamListener):
    def on_error(selcf, status_code):
        if status_code == 402:
            return False
    
    def on_status(self,status):
        tweet = status.text
        tweet = re.sub(r'RT\s@w*:\s','', tweet)
        tweet = re.sub(r'https?.*','', tweet)
        global producer 
        producer.send(KAFKA_TOPIC,bytes(tweet,encoding = 'utf-8'))
        d = datetime.now()
        print(f'[{d.hour}:{d.minute}.{d.second}] sending tweet')

api_key = os.environ["Twitter_API_Key"]
api_secret = os.environ["Twitter_API_Secret"]
access_token = os.environ["Twitter_Access_Key"]
access_token_secret = os.environ["Twitter_Access_Secret"]

auth = tweepy.OAuth1UserHandler(
   consumer_key = api_key, consumer_secret = api_secret, access_token = access_token, access_token_secret = access_token_secret
)

api = tweepy.API(auth)

streamer = Streamer()
stream = tweepy.Stream(auth=api.auth, listener = streamer)

try:
    producer = KafkaProducer(bootstrap_servers = KAFKA_BROKER)
except Exception as e:
    print(f'Error Connection to Kafka: {e}')
    sys.exit(1)

stream.filter(track = TICKER_SYMBOLS)