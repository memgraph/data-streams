from kafka.admin import KafkaAdminClient, NewTopic
from kafka.errors import TopicAlreadyExistsError, NoBrokersAvailable
from time import sleep
import os


KAFKA_IP = os.getenv('KAFKA_IP', 'localhost')
KAFKA_PORT = os.getenv('KAFKA_PORT', '9092')
KAFKA_TOPIC = os.getenv('KAFKA_TOPIC', '')
REDPANDA_IP = os.getenv('REDPANDA_IP', 'localhost')
REDPANDA_PORT = os.getenv('REDPANDA_PORT', '9092')
REDPANDA_TOPIC = os.getenv('REDPANDA_TOPIC', '')


def get_admin_client():
    retries = 30
    while True:
        try:
            admin_client = KafkaAdminClient(
                bootstrap_servers=KAFKA_IP + ':' + KAFKA_PORT,
                client_id="test")
            return admin_client
        except NoBrokersAvailable:
            retries -= 1
            if not retries:
                raise
            sleep(1)


def run():
    admin_client = get_admin_client()
    my_topic = [
        NewTopic(name=KAFKA_TOPIC, num_partitions=1, replication_factor=1)]
    try:
        admin_client.create_topics(new_topics=my_topic, validate_only=False)
    except TopicAlreadyExistsError:
        pass
    print("All topics:")
    print(admin_client.list_topics())


def main():
    run()


if __name__ == "__main__":
    main()
