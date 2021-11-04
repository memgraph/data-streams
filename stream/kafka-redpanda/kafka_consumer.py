from kafka import KafkaConsumer
from time import sleep
import json


def run(ip, port, topic, platform):
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
    except KeyboardInterrupt:
        pass
