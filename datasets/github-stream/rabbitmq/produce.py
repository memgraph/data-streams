from multiprocessing import Process
from time import sleep
import csv
import json
import pika
import os
import rabbitmq_consumer

RABBITMQ_IP = os.getenv('RABBITMQ_IP', 'localhost')
RABBITMQ_PORT = os.getenv('RABBITMQ_PORT', '5672')
RABBITMQ_QUEUE = os.getenv('RABBITMQ_QUEUE', 'github')
DATA = "github-network.csv"


def produce():
    connection = pika.BlockingConnection(pika.ConnectionParameters(RABBITMQ_IP))
    channel = connection.channel()
    channel.queue_declare(queue=RABBITMQ_QUEUE)
    with open(DATA) as sales:
        csvReader = csv.DictReader(sales)
        for rows in csvReader:
            data = {
                'commit': rows['commit'],
                'author': rows['author'],
                'followers': rows['followers'],
                'following': rows['following'],
            }
            channel.basic_publish(
                exchange='', routing_key=RABBITMQ_QUEUE, body=json.dumps(data).encode('utf8'))
            print(" [x] Sent sale!'")
            sleep(1)
        connection.close()


def run():
    p1 = Process(target=lambda: produce())
    p1.start()
    p2 = Process(target=lambda: rabbitmq_consumer.run())
    p2.start()

    p1.join()
    p2.join()


def main():
    run()


if __name__ == "__main__":
    main()
