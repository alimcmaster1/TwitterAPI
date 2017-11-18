import json
from twitter import Twitter, OAuth, TwitterHTTPError, TwitterStream
import Tweet as Tw
import logging
from confluent_kafka import Producer
import _CONSTS as cst

# Auth
oauth = OAuth(cst.ACCESS_TOKEN, cst.ACCESS_SECRET,
              cst.CONSUMER_KEY, cst.CONSUMER_SECRET)


# Initiate the connection to Twitter Streaming API
twitter_stream = TwitterStream(auth=oauth)

iterator = twitter_stream.statuses.filter(track="Google", language="en")

# You don't have to set it to stop, but can continue running
# the Twitter API to collect data for days or even longer.

tweet_count = cst.TWEET_COUNT_LIMIT

p = Producer({'bootstrap.servers': cst.SERVER})

for tweet in iterator:
    tweet_count -= 1

    tweet_read = False
    while not tweet_read:
        try:
            current_tweet = Tw.Tweet(tweet['id'], tweet['timestamp_ms'],
                                     tweet['text'], tweet['place'],
                                     tweet['retweet_count'])
            tweet_read = True
        except KeyError as e:
            logging.warning('Skipping tweet')
            logging.warning(str(e))
            continue

    serialized_json = json.dumps(current_tweet.__dict__)
    print(serialized_json)
    p.produce(cst.KAFKATOPIC, serialized_json.encode('utf8'))

    if tweet_count <= 0:
        break
