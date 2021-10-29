from kafka import KafkaConsumer
from time import sleep
import json
import os


KAFKA_IP = os.getenv('KAFKA_IP', 'localhost')
KAFKA_PORT = os.getenv('KAFKA_PORT', '9092')
KAFKA_TOPIC = os.getenv('KAFKA_TOPIC', 'sales')
REDPANDA_IP = os.getenv('KAFKA_IP', 'localhost')
REDPANDA_PORT = os.getenv('KAFKA_PORT', '9092')
REDPANDA_TOPIC = os.getenv('KAFKA_TOPIC', 'sales')


def run():
    consumer = KafkaConsumer(KAFKA_TOPIC,
                             bootstrap_servers=REDPANDA_IP + ':' + KAFKA_PORT,
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
                    print("Message: " + str(message))
    except KeyboardInterrupt:
        pass


def main():
    run()


if __name__ == "__main__":
    main()
