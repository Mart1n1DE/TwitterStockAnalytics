import tweepy
import os 

api_key = os.environ["Twitter_API_Key"]
api_secret = os.environ["Twitter_API_Secret"]
access_token = os.environ["Twitter_Access_Key"]
access_token_secret = os.environ["Twitter_Access_Secret"]

auth = tweepy.OAuth1UserHandler(
   consumer_key = api_key, consumer_secret = api_secret, access_token = access_token, access_token_secret = access_token_secret
)

api = tweepy.API(auth)

public_tweets = api.home_timeline()
for tweet in public_tweets:
    print(tweet.text)