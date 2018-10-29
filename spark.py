from pyspark import SparkConf, SparkContext
from pyspark.streaming import StreamingContext
import json
import re
from textblob import TextBlob
from elasticsearch import Elasticsearch
from datetime import datetime


TCP_IP = 'localhost'
TCP_PORT = 9001

# Pyspark
# create spark configuration
conf = SparkConf()
conf.setAppName('TwitterApp')
conf.setMaster('local[2]')
conf.set("spark.network.timeout","4200s")
conf.set("spark.executor.heartbeatInterval","4000s")
# create spark context with the above configuration
sc = SparkContext(conf=conf)

# create the Streaming Context from spark context with interval size 2 seconds
ssc = StreamingContext(sc, 4)
ssc.checkpoint("checkpoint_TwitterApp")

es = Elasticsearch([{'host': 'localhost', 'port': 9200}])





def remove_emoji(text_json):
    
    text = text_json['text']
    emoji_pattern = re.compile("["
        u"\U0001F600-\U0001F64F"  
        u"\U0001F300-\U0001F5FF"  
        u"\U0001F680-\U0001F6FF"  
        u"\U0001F1E0-\U0001F1FF"  
                           "]+", flags=re.UNICODE)
    
    text = emoji_pattern.sub(r'', text)
    
    text_json['text'] = text
    
    return(text_json)

def remove_start_atrate(text_json):
    text = text_json['text']
    
    text = re.sub("@[A-Za-z0-9]+"," ",text)
    text_json['text'] = text
    return(text_json)

def remove_special_characters(text_json):
    text = text_json['text']
    text = re.sub("[^0-9A-Za-z \t]"," ",text)
    text_json['text'] = text    
    return(text_json)

def remove_additional_space(text_json):
    text = text_json['text']
    text = " ".join(re.sub("(\w+:\/\/\S+)"," ",text).split())
    text_json['text'] = text 
    return(text_json)

def get_tweet_sentiment(text_json):
    text = text_json['text']
    analysis = TextBlob(text)
    
    sentiment_pol = analysis.sentiment.polarity
    
    if(sentiment_pol > 0):
        text_json['sentiment'] = 'positive'
        return(text_json)
    elif(sentiment_pol == 0):
        text_json['sentiment'] = 'neutral'
        return(text_json)
    else:
        text_json['sentiment'] = 'negative'
        return(text_json)


    
#def processRecord(record):
#    print("hi",record['text'])    
#    return ' '.join(re.sub("(@[A-Za-z0-9]+)|([^0-9A-Za-z \t])|(\w+:\/\/\S+)", " ", tweet).split())

def load_json(x):
    x = json.loads(x)
    return(x)
# read data from port 900
dataStream = ssc.socketTextStream(TCP_IP, TCP_PORT)
dataStream = dataStream.map(lambda x:load_json(x))

dataStream = dataStream.map(lambda x:remove_emoji(x)).map(lambda x:remove_start_atrate(x)).map(lambda x:remove_special_characters(x))
dataStream = dataStream.map(lambda x:remove_additional_space(x))
dataStream = dataStream.map(lambda x:get_tweet_sentiment(x))
#dataStream = dataStream.map(lambda x:index_sentiment(x))

#print(dataStream.collect())

#value_counts.foreachRDD(lambda rdd: rdd.saveAsNewAPIHadoopFile(...))

def sendToES(partition):
#    print("afasfaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa########",type(partition))
#    print(list(partition))
#    try:
    
    
    print("senddddddddddddddddddd")
    tweets = list(partition)
    print(tweets,len(tweets))
    
    es = Elasticsearch([{'host': 'localhost', 'port': 9200}])
    '''
    if(len(tweets) != 0):
            for tweet in tweets:
                
                doc = {
                    'timestamp': datetime.now(),
                    "text": tweet['text'],
                    "location": {
                            "lat": tweet['coordinates'][1],
                            "lon": tweet['coordinates'][0]
                            },
                    "sentiment":tweet['sentiment']
                    
                    }
                es.index(index="location", doc_type='request-info', body=doc)
    '''
    
    
    if(es.indices.exists(index = "location4")):
        print("if")
        if(len(tweets) != 0):
            for tweet in tweets:
                
                doc = {
                    "text": tweet['text'],
                    "location4": {
                            "lat": tweet['coordinates'][1],
                            "lon": tweet['coordinates'][0]
                            },
                    "sentiment":tweet['sentiment']
                    
                    }
                if(tweet['coordinates'][1] != 0 and tweet['coordinates'][0] !=0 ):
                    es.index(index="location4", doc_type='request-info', body=doc)
    else:
        print("else")
        mappings = {
        "mappings": {
            "request-info": {
                "properties": {
                    "text": {
                        "type": "text"
                    },
                    "location4": {
                        "type": "geo_point"
                    },
                    "sentiment": {
                        "type": "text"
                    }                        
                }
            }
        }
    }

        es.indices.create(index='location4', body=mappings)
        if(len(tweets) != 0):
            for tweet in tweets:
                
                doc = {
                    "text": tweet['text'],
                    "location4": {
                            "lat": tweet['coordinates'][1],
                            "lon": tweet['coordinates'][0]
                            },
                    "sentiment":tweet['sentiment']
                    
                    }
                if(tweet['coordinates'][1] != 0 and tweet['coordinates'][0] !=0 ):
                    es.index(index="location4", doc_type='request-info', body=doc)

       
                    
        
        #return True;
#    except:
        #traceback.print_exc();
        #return False;

        
#sentimentTweets=dstream_tweets.map(lambda x: {x:getSentiment(x)});

c=dataStream.foreachRDD(lambda x: x.foreachPartition(lambda y:sendToES(y)))

######### your processing here ###################
'''
dataStream.pprint()
words = dataStream.flatMap(lambda x: x.split(' '))
wordcount = words.map(lambda x: (x,1)).reduceByKey(lambda x,y: x+y)
wordcount.pprint()
'''
#################################################

ssc.start()
ssc.awaitTermination()
