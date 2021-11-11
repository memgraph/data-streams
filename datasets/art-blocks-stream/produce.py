from multiprocessing import Process
from time import sleep
import csv
import json
import kafka_consumer
import kafka_producer as kafka_producers
import kafka_setup
import os
import pika
import pulsar
import pulsar_consumer
import rabbitmq_consumer


KAFKA_IP = os.getenv('KAFKA_IP', 'localhost')
KAFKA_PORT = os.getenv('KAFKA_PORT', '9093')
KAFKA_TOPIC = os.getenv('KAFKA_TOPIC', 'sales')
REDPANDA_IP = os.getenv('REDPANDA_IP', 'localhost')
REDPANDA_PORT = os.getenv('REDPANDA_PORT', '29092')
REDPANDA_TOPIC = os.getenv('REDPANDA_TOPIC', 'sales')
RABBITMQ_IP = os.getenv('RABBITMQ_IP', 'localhost')
RABBITMQ_PORT = os.getenv('RABBITMQ_PORT', '5672')
RABBITMQ_QUEUE = os.getenv('RABBITMQ_QUEUE', 'sales')
PULSAR_IP = os.getenv('PULSAR_IP', 'localhost')
PULSAR_PORT = os.getenv('PULSAR_PORT', '6650')
PULSAR_TOPIC = os.getenv('PULSAR_TOPIC', 'sales')
KAFKA = os.getenv('KAFKA', 'False')
REDPANDA = os.getenv('REDPANDA', 'False')
RABBITMQ = os.getenv('RABBITMQ', 'False')
PULSAR = os.getenv('PULSAR', 'False')
DATA = "data/sales.csv"


def produce_kafka_redpanda(ip, port, topic):
    kafka_producer = kafka_producers.create_kafka_producer(ip, port)
    with open(DATA) as file:
        csvReader = csv.DictReader(file)
        for rows in csvReader:
            sale_id = rows["sale_id"]
            data = {
                sale_id: rows
            }
            kafka_producer.send(topic, json.dumps(
                data[sale_id]).encode('utf8'))
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
            sale_id = rows["sale_id"]
            data = {
                sale_id: rows
            }
            channel.basic_publish(exchange='', routing_key=queue, body=json.dumps(
                data[sale_id]).encode('utf8'))
            sleep(1)
        connection.close()


def produce_pulsar(ip, port, topic):
    print('pulsar://' + ip + ':' + port)
    client = pulsar.Client('pulsar://' + ip + ':' + port)
    producer = client.create_producer(topic)
    with open(DATA) as file:
        csvReader = csv.DictReader(file)
        for rows in csvReader:
            sale_id = rows["sale_id"]
            data = {
                sale_id: rows
            }
            producer.send(json.dumps(
                data[sale_id]).encode('utf8'))
            sleep(1)
        client.close()


def run():
    process_list = list()
    if KAFKA == 'True':
        kafka_setup.run(KAFKA_IP, KAFKA_PORT, KAFKA_TOPIC)

        p1 = Process(target=lambda: produce_kafka_redpanda(
            KAFKA_IP, KAFKA_PORT, KAFKA_TOPIC))
        p1.start()
        process_list.append(p1)
        p2 = Process(target=lambda: kafka_consumer.run(
            KAFKA_IP, KAFKA_PORT, KAFKA_TOPIC, "Kafka"))
        p2.start()
        process_list.append(p2)

    if REDPANDA == 'True':
        p3 = Process(target=lambda: produce_kafka_redpanda(
            REDPANDA_IP, REDPANDA_PORT, REDPANDA_TOPIC))
        p3.start()
        process_list.append(p3)
        p4 = Process(target=lambda: kafka_consumer.run(
            REDPANDA_IP, REDPANDA_PORT, REDPANDA_TOPIC, "Redpanda"))
        p4.start()
        process_list.append(p4)

    if RABBITMQ == 'True':
        p5 = Process(target=lambda: produce_rabbitmq(
            RABBITMQ_IP, RABBITMQ_PORT, RABBITMQ_QUEUE))
        p5.start()
        process_list.append(p5)
        p6 = Process(target=lambda: rabbitmq_consumer.run(
            RABBITMQ_IP, RABBITMQ_PORT, RABBITMQ_QUEUE, "RabbitMQ"))
        p6.start()
        process_list.append(p6)

    if PULSAR == 'True':
        p7 = Process(target=lambda: produce_pulsar(
            PULSAR_IP, PULSAR_PORT, PULSAR_TOPIC))
        p7.start()
        process_list.append(p7)
        p8 = Process(target=lambda: pulsar_consumer.run(
            PULSAR_IP, PULSAR_PORT, PULSAR_TOPIC, "Pulsar"))
        p8.start()
        process_list.append(p8)

    for process in process_list:
        process.join()


def main():
    run()


if __name__ == "__main__":
    main()
