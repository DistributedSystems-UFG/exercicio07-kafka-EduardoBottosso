from kafka import KafkaConsumer, KafkaProducer
from const import *
import sys

# aqui acrescentei a subscricao direta com o TOPIC1, que foi definido nas constantes:
# e ele vai pegar publicacoes antigas tambem
consumer = KafkaConsumer(TOPIC1, bootstrap_servers=[BROKER_ADDR + ':' + BROKER_PORT], auto_offset_reset='earliest')
producer = KafkaProducer(bootstrap_servers=[BROKER_ADDR + ':' + BROKER_PORT])

for msg in consumer:
    received_text = msg.value.decode()
    print ('Recebido de ' + TOPIC1 + ':' + received_text)
    processed_text = 'Processed ' + received_text.upper()
    producer.send(TOPIC2, value=processed_text.encode())
    producer.flush()
