from kafka import KafkaConsumer, KafkaProducer
from const import *
import sys

# aqui acrescentei a subscricao direta com o TOPIC1, que foi definido nas constantes:
# e ele vai pegar publicacoes antigas tambem
consumer = KafkaConsumer(TOPIC1, bootstrap_servers=[BROKER_ADDR + ':' + BROKER_PORT])
producer = KafkaProducer(bootstrap_servers=[BROKER_ADDR + ':' + BROKER_PORT])

for msg in consumer:
    received_text = msg.value.decode()
    print('Received from ' + TOPIC1 + ': ' + received_text)
    
    processed_text = received_text.upper()
    print('Republishing to ' + TOPIC2 + ': ' + processed_text)
    
    producer.send(TOPIC2, value=processed_text.encode())
    producer.flush()
