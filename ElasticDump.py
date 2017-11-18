import json
import random
from confluent_kafka import Consumer, KafkaError
from elasticsearch import Elasticsearch
from elasticsearch import helpers
from datetime import datetime
import _CONSTS as cst
from TweetExperimenting import Tweet as Tw


class elasticPy:

    def __init__(self):
        self.es = Elasticsearch()

    def index_my_tweets(self, index, _type, tweets):
        docs = [{
                  "_index": index,
                  "_type": _type,
                  "_id": self.get_rand_id(),
                  "_source": {
                     "tweet_id": tweet.uniqueId,
                     "create_time": self.convert_timestamp(tweet.creationtimestamp),
                     "text":  tweet.text,
                     "ingestion_timestamp": datetime.now(),
                     "country_of_tweet": tweet.place,
                     "popularity": tweet.popularity}
        } for tweet in tweets]
        self.docs = docs
        return docs

    def push_batch_to_es(self, docs):
        return helpers.bulk(self.es, docs)

    @staticmethod
    def convert_timestamp(timestamp):
        if timestamp is not None:
            return datetime.fromtimestamp(int(timestamp) / 1000)
        else:
            return None

    @staticmethod
    def get_rand_id():
        return int(random.random() * 1e16)


class subscribe():

    def __init__(self, broker, topic):
        self.broker = broker
        self.topic = topic

    def start_sub(self):

        c = Consumer({'bootstrap.servers': self.broker,
                      'group.id': 'mygroup',
                      'default.topic.config': {'auto.offset.reset': 'latest'}})
        c.subscribe([self.topic])

        running = True
        msg_in_mem = []
        while running:
            msg = c.poll(cst.POLL_TIMEOUT_MS)
            if not msg.error():
                decodedmsg = msg.value().decode('utf-8')
                tweet = json.loads(decodedmsg,
                                   object_hook=Tw.Tweet.object_decoder)
                msg_in_mem.append(tweet)
                print('Received message: {}'.format(decodedmsg))
            elif msg.error().code() != KafkaError._PARTITION_EOF:
                print(msg.error())
                running = False

            if len(msg_in_mem) > 10:
                espy = elasticPy()
                docs = espy.index_my_tweets(cst.INDEX, cst.TYPE, msg_in_mem)
                batch_status = espy.push_batch_to_es(docs)
                msg_in_mem = []
                print(batch_status)

        c.close()

if __name__ == '__main__':
    sub = subscribe(cst.SERVER, cst.KAFKATOPIC)
    sub.start_sub()
