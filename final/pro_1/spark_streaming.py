from pyspark import SparkConf, SparkContext
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils
from pymongo import MongoClient
import time
import matplotlib.pyplot as plt
import os
from os import path
import json
from wordcloud import WordCloud

os.environ["PYSPARK_PYTHON"] = "python3"
os.environ["PYSPARK_DRIVER_PYTHON"] = "python3"
os.environ[
    'PYSPARK_SUBMIT_ARGS'] = '--packages org.apache.spark:spark-streaming-kafka-0-8_2.11:2.4.0 pyspark-shell'


def main():
    conf = SparkConf().setMaster("local[2]").setAppName("twitterstream")
    sc = SparkContext(conf=conf)

    # Creating a streaming context with batch interval of 10 sec and plotting interval 10 minutes
    while True:
        ssc = StreamingContext(sc, 10)
        ssc.checkpoint("checkpoint")
        stream(ssc, 100)
        make_cloud()
        time.sleep(600)


def make_cloud():
    # get data directory (using getcwd() is needed to support running example in generated IPython notebook)
    d = path.dirname(__file__) if "__file__" in locals() else os.getcwd()

    # Read the whole text.
    con = MongoClient('localhost:27017')
    words = {}
    for x in con['twitter']['word'].find({}, {"_id": 0, "word": 1, "count": 1}):
        words[x["word"]] = x["count"]

    wordcloud = WordCloud(max_font_size=40).generate_from_frequencies(frequencies=words)
    plt.figure()
    plt.imshow(wordcloud, interpolation="bilinear")
    plt.axis("off")
    plt.show()


def make_plot(counts):
    """
    This function plots the counts of positive and negative words for each timestep.
    """
    positiveCounts = []
    negativeCounts = []
    time = []
    print(counts)
    for val in counts:
        if len(val) == 0:
            continue
        positiveTuple = val[0]
        positiveCounts.append(positiveTuple[1])
        negativeTuple = val[1]
        negativeCounts.append(negativeTuple[1])

    for i in range(len(counts)):
        if len(counts[i]) != 0:
            time.append(i)

    posLine = plt.plot(time, positiveCounts, 'bo-', label='Positive')
    negLine = plt.plot(time, negativeCounts, 'go-', label='Negative')
    plt.axis([0, len(counts), 0, max(max(positiveCounts), max(negativeCounts)) + 50])
    plt.xlabel('Time step')
    plt.ylabel('Word count')
    plt.legend(loc='upper left')
    plt.show()


def load_wordlist(filename):
    """
    This function returns a list or set of words from the given filename.
    """
    words = {}
    f = open(filename, 'rU')
    text = f.read()
    text = text.split('\n')
    for line in text:
        words[line] = 1
    f.close()
    return words


def wordSentiment(word, pwords, nwords):
    if word in pwords:
        return ('positive', 1)
    elif word in nwords:
        return ('negative', 1)


def updateFunction(newValues, runningCount):
    if runningCount is None:
        runningCount = 0
    return sum(newValues, runningCount)


def sendRecord(record):
    con = MongoClient('localhost:27017')
    word = record[0]
    count = record[1]
    print(record[0], record[1])
    db = con['twitter']
    collection = db['word']
    # collection.insert({word: count}, check_keys=False)
    collection.insert({"word": word, "count": count})


def stream(ssc, duration):
    kstream = KafkaUtils.createDirectStream(
        ssc, topics=['twitterstream'], kafkaParams={"metadata.broker.list": 'localhost:9092'})
    tweets = kstream.map(lambda x: x[1])
    print(tweets)
    # Each element of tweets will be the text of a tweet.
    # We keep track of a running total counts and print it at every time step.
    words = tweets.flatMap(lambda line: line.split(" "))
    # print(words)
    wordss = words.map(lambda word: (word, 1))
    wordsCounts = wordss.reduceByKey(lambda x, y: x+y)
    wordsCounts.updateStateByKey(updateFunction)
    wordsCounts.foreachRDD(lambda rdd: rdd.foreach(sendRecord))
    # positive = words.map(lambda word: ('Positive', 1) if word in pwords else ('Positive', 0))
    # negative = words.map(lambda word: ('Negative', 1) if word in nwords else ('Negative', 0))
    # allSentiments = positive.union(negative)
    # sentimentCounts = allSentiments.reduceByKey(lambda x, y: x + y)
    # runningSentimentCounts = sentimentCounts.updateStateByKey(updateFunction)
    # runningSentimentCounts.pprint()
    #
    # # The counts variable hold the word counts for all time steps
    # counts = []
    # sentimentCounts.foreachRDD(lambda rdd: rdd.foreach(sendRecord))
    # sentimentCounts.foreachRDD(lambda t, rdd: counts.append(rdd.collect()))

    # Start the computation
    ssc.start()
    ssc.awaitTerminationOrTimeout(duration)
    ssc.stop(stopGraceFully=True)


if __name__ == "__main__":
    main()
