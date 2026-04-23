from kafka import KafkaProducer
from const import *
import sys
    
producer = KafkaProducer(bootstrap_servers=[BROKER_ADDR + ':' + BROKER_PORT])
for i in range(100):
    msg = 'My ' + str(i) + 'message for topic ' + TOPIC1
    print ('Sending message: ' + msg)
    producer.send(TOPIC1, value=msg.encode())

producer.flush()
