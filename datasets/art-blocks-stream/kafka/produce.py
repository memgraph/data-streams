from multiprocessing import Process
from time import sleep
import csv
import json
import kafka_consumer
import kafka_producer as kafka_producers
import kafka_setup
import os


KAFKA_IP = os.getenv('KAFKA_IP', 'localhost')
KAFKA_PORT = os.getenv('KAFKA_PORT', '9092')
KAFKA_TOPIC = os.getenv('KAFKA_TOPIC', 'sales')
REDPANDA_IP = os.getenv('REDPANDA_IP', 'localhost')
REDPANDA_PORT = os.getenv('REDPANDA_PORT', '9092')
REDPANDA_TOPIC = os.getenv('REDPANDA_TOPIC', 'sales')
DATA = "sales.csv"


def produce():
    kafka_producer = kafka_producers.create_kafka_producer()
    with open(DATA) as sales:
        csvReader = csv.DictReader(sales)
        for rows in csvReader:
            sale_id = rows["sale_id"]
            data = {
                sale_id: rows
            }
            kafka_producer.send(KAFKA_TOPIC, json.dumps(
                data[sale_id]).encode('utf8'))
            kafka_producer.flush()
            sleep(1)
        kafka_producer.close()


def run():
    kafka_setup.run()

    p1 = Process(target=lambda: produce())
    p1.start()
    p2 = Process(target=lambda: kafka_consumer.run())
    p2.start()

    p1.join()
    p2.join()


def main():
    run()


if __name__ == "__main__":
    main()
