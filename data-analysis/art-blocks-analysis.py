import os
from kafka import KafkaConsumer
from time import sleep
import json
import pika
import pulsar

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

project_sales = dict()
seler_sales = dict()
buyer_sales = dict()
best_sale = 0
day_sales = dict()
total_sales = 0


def analyze(message):
    # ----TOTAL SALES----
    global total_sales
    total_sales += 1
    print("Total number of sales: " + str(total_sales))
    print("------------------------------------------------------")

    # ----BEST PROJECT----
    global project_sales
    project_id = str(message["project_id"])
    if project_id in project_sales:
        project_sales[project_id] += 1
    else:
        project_sales[project_id] = 1
    best_project = max(project_sales, key=project_sales.get)
    print("Project with largest number of sales: " + str(best_project))
    print("Number of sales: " + str(project_sales[best_project]))
    print("------------------------------------------------------")

    # ----BEST SELLER----
    global seler_sales
    seller_id = str(message["seller_id"])
    if seller_id in seler_sales:
        seler_sales[seller_id] += 1
    else:
        seler_sales[seller_id] = 1
    best_seller = max(seler_sales, key=seler_sales.get)
    print("Seller with largest number of sales: " + str(best_seller))
    print("Number of sales: " + str(seler_sales[best_seller]))
    print("------------------------------------------------------")

    # ----BEST BUYER----
    global buyer_sales
    buyer_id = str(message["buyer_id"])
    if buyer_id in buyer_sales:
        buyer_sales[buyer_id] += 1
    else:
        buyer_sales[buyer_id] = 1
    best_buyer = max(buyer_sales, key=buyer_sales.get)
    print("Buyer with largest number of sales: " + str(best_buyer))
    print("Number of buys: " + str(buyer_sales[best_buyer]))
    print("------------------------------------------------------")

    # ----BEST SALE----
    global best_sale
    price = int(message["price"])
    if price > best_sale:
        best_sale = price
    print("Best sale price is: " + str(best_sale))
    print("Sale id: " + str(message["sale_id"]))
    print("------------------------------------------------------")

    # ----BEST DAY----
    global day_sales
    datetime = str(message["datetime"])
    date = datetime.split(" ")[0]
    if date in day_sales:
        day_sales[date] += 1
    else:
        day_sales[date] = 1
    best_day = max(day_sales, key=day_sales.get)
    print("Day with largest number of sales: " + str(best_day))
    print("Number of sales: " + str(day_sales[best_day]))
    print("------------------------------------------------------")


def consume_kafka_redpanda(ip, port, topic, platform):
    print("Running kafka consumer")
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
                    analyze(message)

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
