from multiprocessing import Process
from time import sleep
import csv
import json
import kafka_consumer
import kafka_producer as kafka_producers
import kafka_setup
import os
import pika
import rabbitmq_consumer


KAFKA_IP = os.getenv('KAFKA_IP', 'localhost')
KAFKA_PORT = os.getenv('KAFKA_PORT', '9092')
KAFKA_TOPIC = os.getenv('KAFKA_TOPIC', 'github')
REDPANDA_IP = os.getenv('REDPANDA_IP', 'localhost')
REDPANDA_PORT = os.getenv('REDPANDA_PORT', '29092')
REDPANDA_TOPIC = os.getenv('REDPANDA_TOPIC', 'github')
RABBITMQ_IP = os.getenv('RABBITMQ_IP', 'localhost')
RABBITMQ_PORT = os.getenv('RABBITMQ_PORT', '5672')
RABBITMQ_QUEUE = os.getenv('RABBITMQ_QUEUE', 'github')
DATA = "data/github-network.csv"


def produce_kafka_redpanda(ip, port, topic):
    kafka_producer = kafka_producers.create_kafka_producer(ip, port)
    with open(DATA) as file:
        csvReader = csv.DictReader(file)
        for rows in csvReader:
            data = {
                'commit': rows['commit'],
                'author': rows['author'],
                'followers': rows['followers'],
                'following': rows['following'],
            }
            kafka_producer.send(topic, json.dumps(data).encode('utf8'))
            kafka_producer.flush()
            sleep(1)
        kafka_producer.close()


def produce_rabbitmq(ip, port, queue):
    connection = pika.BlockingConnection(
        pika.ConnectionParameters(ip))
    channel = connection.channel()
    channel.queue_declare(queue=queue)
    with open(DATA) as file:
        csvReader = csv.DictReader(file)
        for rows in csvReader:
            data = {
                'commit': rows['commit'],
                'author': rows['author'],
                'followers': rows['followers'],
                'following': rows['following'],
            }
            channel.basic_publish(
                exchange='', routing_key=queue, body=json.dumps(data).encode('utf8'))
            sleep(1)
        connection.close()


def run():
    kafka_setup.run(KAFKA_IP, KAFKA_PORT, KAFKA_TOPIC)

    p1 = Process(target=lambda: produce_kafka_redpanda(
        KAFKA_IP, KAFKA_PORT, KAFKA_TOPIC))
    p1.start()
    p2 = Process(target=lambda: kafka_consumer.run(
        KAFKA_IP, KAFKA_PORT, KAFKA_TOPIC, "Kafka"))
    p2.start()

    p3 = Process(target=lambda: produce_kafka_redpanda(
        REDPANDA_IP, REDPANDA_PORT, REDPANDA_TOPIC))
    p3.start()
    p4 = Process(target=lambda: kafka_consumer.run(
        REDPANDA_IP, REDPANDA_PORT, REDPANDA_TOPIC, "Redpanda"))
    p4.start()

    p5 = Process(target=lambda: produce_rabbitmq(
        RABBITMQ_IP, RABBITMQ_PORT, RABBITMQ_QUEUE))
    p5.start()
    p6 = Process(target=lambda: rabbitmq_consumer.run(
        RABBITMQ_IP, RABBITMQ_PORT, RABBITMQ_QUEUE, "RabbitMQ"))
    p6.start()

    p1.join()
    p2.join()
    p3.join()
    p4.join()
    p5.join()
    p6.join()


def main():
    run()


if __name__ == "__main__":
    main()
