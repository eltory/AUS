import os

os.environ[
    'PYSPARK_SUBMIT_ARGS'] = '--packages org.apache.spark:spark-streaming-kafka-0-8_2.11:2.3.0 pyspark-shell'
# '--jars /home/Downloads/spark-streaming-kafka-0-8-assembly_2.11-2.3.0.jar pyspark-shell'

import sys
import time
from pyspark import SparkContext, SparkConf
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils

n_secs = 1
topic = "cloud"

conf = SparkConf().setAppName("KafkaStreamProcessor").setMaster("local[*]")
sc = SparkContext(conf=conf)
sc.setLogLevel("WARN")
ssc = StreamingContext(sc, n_secs)

kafkaStream = KafkaUtils.createDirectStream(ssc, [topic], {
    'bootstrap.servers': 'localhost:9092',
    'group.id': 'video-group',
    'fetch.message.max.bytes': '15728640',
    'auto.offset.reset': 'largest'})
# Group ID is completely arbitrary

lines = kafkaStream.map(lambda x: x[1])
counts = lines.flatMap(lambda line: line.split(" ")).map(lambda word: (word, 1)).reduceByKey(
    lambda a, b: a + b)
counts.pprint()

ssc.start()
time.sleep(600)  # Run stream for 10 minutes just in case no detection of producer
# ssc.awaitTermination()
ssc.stop(stopSparkContext=True, stopGraceFully=True)