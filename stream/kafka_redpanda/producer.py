from kafka import KafkaProducer
from kafka.errors import NoBrokersAvailable
from time import sleep


def create_kafka_producer(ip, port):
    retries = 30
    while True:
        try:
            producer = KafkaProducer(
                bootstrap_servers=ip + ':' + port)
            return producer
        except NoBrokersAvailable:
            retries -= 1
            if not retries:
                raise
            print("Failed to connect to Kafka")
            sleep(1)
