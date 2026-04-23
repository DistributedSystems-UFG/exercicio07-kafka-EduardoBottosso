from kafka import KafkaProducer
from const import *
import sys
    
producer = KafkaProducer(bootstrap_servers=[BROKER_ADDR + ':' + BROKER_PORT])
for i in range(100):
    msg = f'Event {i} | source=producer1 | stage=raw | topic={TOPIC1}'
    print('Sending to ' + TOPIC1 + ': ' + msg)
    producer.send(TOPIC1, value=msg.encode())

producer.flush()
