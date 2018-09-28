# coding: utf-8
import csv
import time
from kafka import KafkaProducer

# 实例化一个KafkaProducer示例，用于向Kafka投递消息
producer = KafkaProducer(bootstrap_servers='192.168.152.12:9092,192.168.152.13:9092,192.168.152.14:9092')
# 打开数据文件
#csvfile = open("../data/test.csv", "r")
csvfile = open("../../user_log.csv", "r", encoding='UTF-8')
# 生成一个可用于读取csv文件的reader
reader = csv.reader(csvfile)

for line in reader:
    gender = line[9]  # 性别在每行日志代码的第10个元素
    if gender == 'gender':
        continue  # 去除第一行表头
    time.sleep(0.01)  # 每隔0.1秒发送一行数据
    # 发送数据，topic为'sex'
    producer.send('sex', line[9].encode('utf8'))