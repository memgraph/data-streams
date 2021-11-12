import os
from kafka import KafkaConsumer
from time import sleep
import json
import pika
import pulsar

KAFKA_IP = os.getenv('KAFKA_IP', 'localhost')
KAFKA_PORT = os.getenv('KAFKA_PORT', '9093')
KAFKA_TOPIC = os.getenv('KAFKA_TOPIC', 'github')
REDPANDA_IP = os.getenv('REDPANDA_IP', 'localhost')
REDPANDA_PORT = os.getenv('REDPANDA_PORT', '29092')
REDPANDA_TOPIC = os.getenv('REDPANDA_TOPIC', 'github')
RABBITMQ_IP = os.getenv('RABBITMQ_IP', 'localhost')
RABBITMQ_PORT = os.getenv('RABBITMQ_PORT', '5672')
RABBITMQ_QUEUE = os.getenv('RABBITMQ_QUEUE', 'github')
PULSAR_IP = os.getenv('PULSAR_IP', 'localhost')
PULSAR_PORT = os.getenv('PULSAR_PORT', '6650')
PULSAR_TOPIC = os.getenv('PULSAR_TOPIC', 'github')
KAFKA = os.getenv('KAFKA', 'False')
REDPANDA = os.getenv('REDPANDA', 'False')
RABBITMQ = os.getenv('RABBITMQ', 'False')
PULSAR = os.getenv('PULSAR', 'False')


def consume_kafka_redpanda(ip, port, topic, platform):
    print("Running kafka consumer")
    total = 0
    consumer = KafkaConsumer(topic,
                             bootstrap_servers=ip + ':' + port,
                             auto_offset_reset='earliest',
                             group_id=None)
    try:
        while True:
            msg_pack = consumer.poll()
            if not msg_pack:
                sleep(1)
                continue
            for _, messages in msg_pack.items():
                for message in messages:
                    message = json.loads(message.value.decode('utf8'))
                    print(platform, " :", str(message))
                    total += 1
                    print("Total number of messages: " + str(total))

    except KeyboardInterrupt:
        pass


def consume_rabbitmq(ip, port, queue, platform):
    connection = pika.BlockingConnection(
        pika.ConnectionParameters(host=ip))
    channel = connection.channel()

    channel.queue_declare(queue=queue)

    def callback(ch, method, properties, body):
        print(platform, ": ", str(body))

    channel.basic_consume(
        queue=queue, on_message_callback=callback, auto_ack=True)

    print(' [*] Waiting for messages. To exit press CTRL+C')
    channel.start_consuming()


def consume_pulsar(ip, port, topic, platform):
    client = pulsar.Client('pulsar://' + ip + ':' + port)

    consumer = client.subscribe(topic, 'my-subscription')

    while True:
        msg = consumer.receive()
        try:
            print(platform, ": ", msg.data())
            # Acknowledge successful processing of the message
            consumer.acknowledge(msg)
        except:
            # Message failed to be processed
            consumer.negative_acknowledge(msg)
            client.close()


def main():
    if KAFKA == 'True':
        consume_kafka_redpanda(KAFKA_IP, KAFKA_PORT, KAFKA_TOPIC, "Kafka")
    elif REDPANDA == 'True':
        consume_kafka_redpanda(REDPANDA_IP, REDPANDA_PORT,
                               REDPANDA_TOPIC, "Redpanda")
    elif RABBITMQ == 'True':
        consume_rabbitmq(RABBITMQ_IP, REDPANDA_PORT, REDPANDA_TOPIC, "RabbitMQ")
    elif PULSAR == 'True':
        consume_pulsar(PULSAR_IP, PULSAR_PORT, PULSAR_TOPIC, "Pulsar")


if __name__ == "__main__":
    main()
