from pyspark import SparkConf, SparkContext
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils
from pymongo import MongoClient
import time
import matplotlib.pyplot as plt
import os
from os import path
from wordcloud import WordCloud

# Pyspark 환경설정
os.environ["PYSPARK_PYTHON"] = "python3"
os.environ["PYSPARK_DRIVER_PYTHON"] = "python3"
os.environ['PYSPARK_SUBMIT_ARGS'] = \
    '--packages org.apache.spark:spark-streaming-kafka-0-8_2.11:2.4.0 pyspark-shell'


def main():
    """
    메인
    :return: None
    """
    # Spark Stream / 두 개의 로컬에서 작업
    conf = SparkConf().setMaster("local[2]").setAppName("twitter_stream")
    sc = SparkContext(conf=conf)
    pwords = load_word_list("./Dataset/positive.txt")
    nwords = load_word_list("./Dataset/negative.txt")

    # Interval 기준으로 데이터 시각화
    while True:
        # Interval 10초
        ssc = StreamingContext(sc, 10, pwords, nwords)
        ssc.checkpoint("checkpoint")
        stream(ssc, 100)
        make_cloud()
        time.sleep(600)


def make_cloud():
    """
    데이터 시각화 - 단어 출현 횟수
    :return: None
    """
    # get data directory (using getcwd() is needed to support running example in generated IPython notebook)
    d = path.dirname(__file__) if "__file__" in locals() else os.getcwd()

    # 몽고 DB 연결, 단어 갯수 시각화
    con = MongoClient('localhost:27017')
    words = {}
    for x in con['twitter']['word'].find({}, {"_id": 0, "word": 1, "count": 1}):
        words[x["word"]] = x["count"]

    word_cloud = WordCloud(max_font_size=40).generate_from_frequencies(frequencies=words)
    plt.figure()
    plt.imshow(word_cloud, interpolation="bilinear")
    plt.axis("off")
    plt.show()


def make_plot(counts):
    """
    데이터 시각화 - 트위터 긍정 / 부정
    :param counts:
    :return: None
    """
    positive_counts = []
    negative_counts = []
    time = []

    for val in counts:
        if len(val) == 0:
            continue
        positive_tuple = val[0]
        negative_tuple = val[1]
        positive_counts.append(positive_tuple[1])
        negative_counts.append(negative_tuple[1])

    for i in range(len(counts)):
        if len(counts[i]) != 0:
            time.append(i)

    pos_line = plt.plot(time, positive_counts, 'bo-', label='Positive')
    neg_line = plt.plot(time, negative_counts, 'go-', label='Negative')
    plt.axis([0, len(counts), 0, max(max(positive_counts), max(negative_counts)) + 50])
    plt.xlabel('Time step')
    plt.ylabel('Word count')
    plt.legend(loc='upper left')
    plt.show()


def load_word_list(filename):
    """
    Word list 불러오기
    :param filename: 파일경로
    :return: 단어 리스트
    """
    words = {}
    f = open(filename, 'rU')
    text = f.read()
    text = text.split('\n')
    for line in text:
        words[line] = 1
    f.close()
    return words


def word_sentiment(word, pwords, nwords):
    """
    긍정 부정 단어 판단
    :param word: 기준 단어
    :param pwords: Positive word 목록
    :param nwords: Negative word 목록
    :return: 긍정 / 부정
    """
    if word in pwords:
        return ('positive', 1)
    elif word in nwords:
        return ('negative', 1)


def value_update(new_values, running_count):
    """
    Value update
    :param new_values: 카운트 값
    :param running_count: 추가될 값
    :return: 카운트 값 합
    """
    if running_count is None:
        running_count = 0
    return sum(new_values, running_count)


def send_record(record):
    """
    Mongo DB에 레코드 저장
    :param record: word, count
    :return: None
    """
    con = MongoClient('localhost:27017')
    word = record[0]
    count = record[1]

    db = con['twitter']
    collection = db['word']
    collection.insert({"word": word, "count": count})


def stream(ssc, duration, pwords, nwords):
    """
    Kafka
    :param ssc: Streaming Context
    :param duration: Interval
    :return: None
    """
    kafka_stream = KafkaUtils.createDirectStream(ssc,
                                                 topics=['twitter_stream'],
                                                 kafkaParams={"metadata.broker.list":'localhost:9092'})
    # 실시간 트위터 가져오기
    tweets = kafka_stream.map(lambda x: x[1])

    # Word 나누기
    words = tweets.flatMap(lambda line: line.split(" "))

    # Pair 로 맵 구성
    pairs = words.map(lambda word: (word, 1))

    # 각각의 글자별 나타난 횟수 업데이트
    words_counts = pairs.reduceByKey(lambda x, y: x+y)
    words_counts.updateStateByKey(value_update)
    words_counts.foreachRDD(lambda rdd: rdd.foreach(send_record))

    # positive = words.map(lambda word: ('Positive', 1) if word in pwords else ('Positive', 0))
    # negative = words.map(lambda word: ('Negative', 1) if word in nwords else ('Negative', 0))
    #
    # all_sentiments = positive.union(negative)
    # sentiment_counts = all_sentiments.reduceByKey(lambda x, y: x + y)
    # running_sentiment_counts = sentiment_counts.updateStateByKey(value_update)
    # running_sentiment_counts.pprint()
    #
    # counts = []
    # sentiment_counts.foreachRDD(lambda rdd: rdd.foreach(send_record))
    # sentiment_counts.foreachRDD(lambda t, rdd: counts.append(rdd.collect()))
    #
    # ssc.start()
    # ssc.awaitTerminationOrTimeout(duration)
    # ssc.stop(stopGraceFully=True)


if __name__ == "__main__":
    main()
