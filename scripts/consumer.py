# coding: utf-8
from kafka import KafkaConsumer

consumer = KafkaConsumer('sex',bootstrap_servers=['192.168.152.12:9092','192.168.152.13:9092','192.168.152.14:9092'])
for msg in consumer:
    print((msg.value).decode('utf8'))

