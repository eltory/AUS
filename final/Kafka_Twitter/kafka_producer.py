from kafka import SimpleProducer
from kafka.client import KafkaClient
import tweepy
import configparser


class TweeterStreamListener(tweepy.StreamListener):
    """
    트위터에서 발생되는 실시간 트위터 스트림을 Kafka Producer 를 통해 kafka queue 로 publish
    """

    def __init__(self, api):
        """
        Kafka Simple Producer 생성
        :param api: tweepy api
        """
        super(tweepy.StreamListener, self).__init__()
        client = KafkaClient('localhost:9092')
        self.api = api
        self.producer = SimpleProducer(client,
                                       async_=True, batch_send_every_n=1000,
                                       batch_send_every_t=10)

    def on_status(self, status):
        """
        트위터에서 실시간으로 발생하는 twitter stream 을
        Kafka Producer 를 통해 'twitter_stream' topic 으로 publish
        :param status:
        :return: Message push 성공 여부
        """
        msg = status.text.encode('utf-8')
        try:
            # topic = 'twitter_stream'
            self.producer.send_messages('twitter_stream', msg)
        except Exception as e:
            print(e)
            return False
        return True

    def on_error(self, status_code):
        print(status_code)
        print("Error received in kafka producer")
        return True

    def on_timeout(self):
        return True


if __name__ == '__main__':

    # twitter.txt 에서 Twitter Api 사용을 위한 credential 읽어오기
    config = configparser.ConfigParser()
    config.read('twitter.txt')

    consumer_key = config['DEFAULT']['consumerKey']
    consumer_secret = config['DEFAULT']['consumerSecret']
    access_key = config['DEFAULT']['accessToken']
    access_secret = config['DEFAULT']['accessTokenSecret']

    # Auth object 생성
    auth = tweepy.OAuthHandler(consumer_key, consumer_secret)
    auth.set_access_token(access_key, access_secret)

    # Twitter tweepy api 연동
    api = tweepy.API(auth,
                     wait_on_rate_limit=True,  # 트위터 api 호 제한 횟수 보충을 기다릴지 여부
                     wait_on_rate_limit_notify=True,  # 트위터 api 호 제한 횟수 보충을 기다릴 때, 따로 안내 메세지 출력
                     compression=True)  # Gzip 압축 사용할지 여부

    # 스트리밍 세션 설정, listener 인스턴스에 메세지 스트리밍
    stream = tweepy.Stream(auth, listener=TweeterStreamListener(api))

    # 전세계 / 영어
    # 참고로 남한 커버 = [125.06666667, 33.10000000, 131.87222222, 38.45000000]
    stream.filter(locations=[-180, -90, 180, 90], languages=['en'])
