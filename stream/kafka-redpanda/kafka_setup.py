from kafka.admin import KafkaAdminClient, NewTopic
from kafka.errors import TopicAlreadyExistsError, NoBrokersAvailable
from time import sleep


def get_admin_client(ip, port):
    retries = 30
    while True:
        try:
            admin_client = KafkaAdminClient(
                bootstrap_servers=ip + ':' + port,
                client_id="test")
            return admin_client
        except NoBrokersAvailable:
            retries -= 1
            if not retries:
                raise
            sleep(1)


def run(ip, port, topic):
    admin_client = get_admin_client(ip, port)
    my_topic = [
        NewTopic(name=topic, num_partitions=1, replication_factor=1)]
    try:
        admin_client.create_topics(new_topics=my_topic, validate_only=False)
    except TopicAlreadyExistsError:
        pass
    print("All topics:")
    print(admin_client.list_topics())
