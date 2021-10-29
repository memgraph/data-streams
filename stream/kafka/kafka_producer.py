from kafka import KafkaProducer
from kafka.errors import NoBrokersAvailable
from time import sleep
import os


KAFKA_IP = os.getenv('KAFKA_IP', 'localhost')
KAFKA_PORT = os.getenv('KAFKA_PORT', '9092')
KAFKA_TOPIC = os.getenv('KAFKA_TOPIC', 'sales')
REDPANDA_IP = os.getenv('REDPANDA_IP', 'localhost')
REDPANDA_PORT = os.getenv('REDPANDA_PORT', '9092')
REDPANDA_TOPIC = os.getenv('REDPANDA_TOPIC', 'sales')
DATA = "github-network.csv"


def create_kafka_producer():
    retries = 30
    while True:
        try:
            producer = KafkaProducer(
                bootstrap_servers=KAFKA_IP + ':' + KAFKA_PORT)
            return producer
        except NoBrokersAvailable:
            retries -= 1
            if not retries:
                raise
            print("Failed to connect to Kafka")
            sleep(1)
