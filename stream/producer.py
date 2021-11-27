from multiprocessing import Process
from time import sleep
import json
import stream.kafka_redpanda.consumer as kafka_consumer
import stream.kafka_redpanda.producer as kafka_producer
import stream.kafka_redpanda.setup as kafka_setup
import os
import pika
import pulsar
import stream.pulsar.consumer as pulsar_consumer
import stream.rabbitmq.consumer as rabbitmq_consumer


KAFKA_IP = os.getenv('KAFKA_IP', 'localhost')
KAFKA_PORT = os.getenv('KAFKA_PORT', '9093')
KAFKA_TOPIC = os.getenv('KAFKA_TOPIC', 'movielens')
REDPANDA_IP = os.getenv('REDPANDA_IP', 'localhost')
REDPANDA_PORT = os.getenv('REDPANDA_PORT', '29092')
REDPANDA_TOPIC = os.getenv('REDPANDA_TOPIC', 'movielens')
RABBITMQ_IP = os.getenv('RABBITMQ_IP', 'localhost')
RABBITMQ_PORT = os.getenv('RABBITMQ_PORT', '5672')
RABBITMQ_QUEUE = os.getenv('RABBITMQ_QUEUE', 'movielens')
PULSAR_IP = os.getenv('PULSAR_IP', 'localhost')
PULSAR_PORT = os.getenv('PULSAR_PORT', '6650')
PULSAR_TOPIC = os.getenv('PULSAR_TOPIC', 'movielens')
KAFKA = os.getenv('KAFKA', 'False')
REDPANDA = os.getenv('REDPANDA', 'False')
RABBITMQ = os.getenv('RABBITMQ', 'False')
PULSAR = os.getenv('PULSAR', 'False')


def produce_kafka_redpanda(ip, port, topic, generate):
    producer = kafka_producer.create_kafka_producer(ip, port)
    message = generate()
    while True:
        try:
            producer.send(topic, json.dumps(next(message)).encode('utf8'))
            producer.flush()
            sleep(1)
        except Exception as e:
            print(f"Error: {e}")
    # kafka_producer.close()


def produce_rabbitmq(ip, port, queue, generate):
    connection = pika.BlockingConnection(
        pika.ConnectionParameters(ip))
    channel = connection.channel()
    channel.queue_declare(queue=queue)
    message = generate()
    while True:
        try:
            channel.basic_publish(
                exchange='', routing_key=queue, body=json.dumps(next(message)).encode('utf8'))
            sleep(1)
        except Exception as e:
            print(f"Error: {e}")
    # connection.close()


def produce_pulsar(ip, port, topic, generate):
    client = pulsar.Client('pulsar://' + ip + ':' + port)
    producer = client.create_producer(topic)
    message = generate()
    while True:
        try:
            producer.send(json.dumps(next(message)).encode('utf8'))
            sleep(1)
        except Exception as e:
            print(f"Error: {e}")
    # client.close()


def run(generate):
    process_list = list()
    if KAFKA == 'True':
        kafka_setup.run(KAFKA_IP, KAFKA_PORT, KAFKA_TOPIC)

        p1 = Process(target=lambda: produce_kafka_redpanda(
            KAFKA_IP, KAFKA_PORT, KAFKA_TOPIC, generate))
        p1.start()
        process_list.append(p1)
        """
        p2 = Process(target=lambda: kafka_consumer.run(
            KAFKA_IP, KAFKA_PORT, KAFKA_TOPIC, "Kafka"))
        p2.start()
        process_list.append(p2)
        """

    if REDPANDA == 'True':
        p3 = Process(target=lambda: produce_kafka_redpanda(
            REDPANDA_IP, REDPANDA_PORT, REDPANDA_TOPIC, generate))
        p3.start()
        process_list.append(p3)
        p4 = Process(target=lambda: kafka_consumer.run(
            REDPANDA_IP, REDPANDA_PORT, REDPANDA_TOPIC, "Redpanda"))
        p4.start()
        process_list.append(p4)

    if RABBITMQ == 'True':
        p5 = Process(target=lambda: produce_rabbitmq(
            RABBITMQ_IP, RABBITMQ_PORT, RABBITMQ_QUEUE, generate))
        p5.start()
        process_list.append(p5)
        """
        p6 = Process(target=lambda: rabbitmq_consumer.run(
            RABBITMQ_IP, RABBITMQ_PORT, RABBITMQ_QUEUE, "RabbitMQ"))
        p6.start()
        process_list.append(p6)
        """

    if PULSAR == 'True':
        p7 = Process(target=lambda: produce_pulsar(
            PULSAR_IP, PULSAR_PORT, PULSAR_TOPIC, generate))
        p7.start()
        process_list.append(p7)
        """
        p8 = Process(target=lambda: pulsar_consumer.run(
            PULSAR_IP, PULSAR_PORT, PULSAR_TOPIC, "Pulsar"))
        p8.start()
        process_list.append(p8)
        """

    for process in process_list:
        process.join()
