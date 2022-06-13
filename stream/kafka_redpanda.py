from kafka import KafkaConsumer, KafkaProducer
from kafka.admin import KafkaAdminClient, NewTopic
from kafka.errors import TopicAlreadyExistsError, NoBrokersAvailable
from time import sleep
import json


def get_admin_client(ip, port, kafka_username, kafka_password):
    retries = 30
    while True:
        try:
            admin_client = KafkaAdminClient(
                bootstrap_servers=ip + ':' + port,
                client_id="test",
                security_protocol="SASL_PLAINTEXT",
                sasl_mechanism="PLAIN",
                sasl_plain_username=kafka_username,
                sasl_plain_password=kafka_password)
            return admin_client
        except NoBrokersAvailable:
            retries -= 1
            if not retries:
                raise
            sleep(1)


def consumer(ip, port, topic, platform):
    consumer = KafkaConsumer(topic,
                             bootstrap_servers=ip + ':' + port,
                             auto_offset_reset='earliest',
                             group_id=None,
                             security_protocol="SASL_PLAINTEXT",
                             sasl_mechanism="PLAIN",
                             sasl_plain_username="public",
                             sasl_plain_password="public")
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

    except Exception as e:
            print(f"Error: {e}")


def create_topic(ip, port, topic, kafka_username, kafka_password):
    admin_client = get_admin_client(ip, port, kafka_username, kafka_password)
    my_topic = [
        NewTopic(name=topic, num_partitions=1, replication_factor=1)]
    try:
        admin_client.create_topics(new_topics=my_topic, validate_only=False)
    except TopicAlreadyExistsError:
        pass
    print(f"All topics: {admin_client.list_topics()}")


def create_kafka_producer(ip, port, kafka_username, kafka_password):
    retries = 30
    while True:
        try:
            producer = KafkaProducer(
                bootstrap_servers=ip + ':' + port,
                security_protocol="SASL_PLAINTEXT",
                sasl_mechanism="PLAIN",
                sasl_plain_username=kafka_username,
                sasl_plain_password=kafka_password)
            return producer
        except NoBrokersAvailable:
            retries -= 1
            if not retries:
                raise
            print("Failed to connect to Kafka")
            sleep(1)


def producer(ip, port, topic, kafka_username, kafka_password, generate, stream_delay):
    producer = create_kafka_producer(ip, port, kafka_username, kafka_password)
    message = generate()
    while True:
        try:
            mssg = json.dumps(next(message)).encode('utf8')
            producer.send(topic, mssg)
            print(mssg)
            producer.flush()
            sleep(stream_delay)
        except Exception as e:
            print(f"Error: {e}")
