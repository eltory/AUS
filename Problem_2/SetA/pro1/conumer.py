from kafka import KafkaConsumer
from json import loads
import matplotlib.pyplot as plt


# kafka consumer connect
consumer = KafkaConsumer(
    'pro_1',
    bootstrap_servers=['localhost:9092'],
    auto_offset_reset='earliest',
    enable_auto_commit=True,
    group_id='my_group',
    value_deserializer=lambda x: loads(x.decode('utf-8')))


fig = plt.figure()
ax = fig.add_subplot(111)
i = 0
a, b = [], []
plt.show()
# receive data from kafka producer and draw real time graph
for message in consumer:
    message = message.value
    print(message)
    a.append(i)
    b.append(message['data'])
    ax.clear()
    ax.plot(a, b, color='r')
    ax.set_xlim(left=max(0, i - 10), right=i + 1)
    fig.show()
    plt.close(fig)
    i += 1
