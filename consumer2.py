from kafka import KafkaConsumer
from const import *

consumer = KafkaConsumer(TOPIC2, bootstrap_servers=[BROKER_ADDR + ':' + BROKER_PORT], auto_offset_reset='earliest')

for msg in consumer:
	print (msg.value.decode())
