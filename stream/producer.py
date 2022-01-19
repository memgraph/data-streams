from multiprocessing import Process
import argparse
import os
import pika
import pulsar
import stream.apache_pulsar as apache_pulsar
import stream.kafka_redpanda as kafka_redpanda
import stream.rabbitmq as rabbitmq

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


def restricted_float(x):
    try:
        x = float(x)
    except ValueError:
        raise argparse.ArgumentTypeError("%r not a floating-point literal" % (x,))
    if x < 0.0 or x > 3.0:
        raise argparse.ArgumentTypeError("%r not in range [0.0, 3.0]" % (x,))
    return x


def str2bool(v):
    if isinstance(v, bool):
        return v
    if v.lower() in ('yes', 'true', 't', 'y', '1'):
        return True
    elif v.lower() in ('no', 'false', 'f', 'n', '0'):
        return False
    else:
        raise argparse.ArgumentTypeError('Boolean value expected.')


def parse_arguments():
    parser = argparse.ArgumentParser()
    parser.add_argument("--stream-delay", type=restricted_float, default=2.0,
                        help="Seconds to wait before producing a new message (MIN=0.0, MAX=3.0).")
    parser.add_argument("--consumer", type=str2bool, nargs='?', const=True, default=False,
                        help="Start consumers.")
    value = parser.parse_args()
    return value


def run(generate):
    args = parse_arguments()
    process_list = list()

    if KAFKA == 'True':
        kafka_redpanda.create_topic(KAFKA_IP, KAFKA_PORT, KAFKA_TOPIC)

        p1 = Process(target=lambda: kafka_redpanda.producer(
            KAFKA_IP, KAFKA_PORT, KAFKA_TOPIC, generate, args.stream_delay))
        p1.start()
        process_list.append(p1)

        if args.consumer:
            p2 = Process(target=lambda: kafka_redpanda.consumer(
                KAFKA_IP, KAFKA_PORT, KAFKA_TOPIC, "Kafka"))
            p2.start()
            process_list.append(p2)

    if REDPANDA == 'True':
        p3 = Process(target=lambda: kafka_redpanda.producer(
            REDPANDA_IP, REDPANDA_PORT, REDPANDA_TOPIC, generate, args.stream_delay))
        p3.start()
        process_list.append(p3)

        if args.consumer:
            p4 = Process(target=lambda: kafka_redpanda.consumer(
                REDPANDA_IP, REDPANDA_PORT, REDPANDA_TOPIC, "Redpanda"))
            p4.start()
            process_list.append(p4)

    if RABBITMQ == 'True':
        p5 = Process(target=lambda: rabbitmq.producer(
            RABBITMQ_IP, RABBITMQ_PORT, RABBITMQ_QUEUE, generate, args.stream_delay))
        p5.start()
        process_list.append(p5)

        if args.consumer:
            p6 = Process(target=lambda: rabbitmq.consumer(
                RABBITMQ_IP, RABBITMQ_PORT, RABBITMQ_QUEUE, "RabbitMQ"))
            p6.start()
            process_list.append(p6)

    if PULSAR == 'True':
        p7 = Process(target=lambda: apache_pulsar.producer(
            PULSAR_IP, PULSAR_PORT, PULSAR_TOPIC, generate, args.stream_delay))
        p7.start()
        process_list.append(p7)

        #if args.consumer:
        #    p8 = Process(target=lambda: apache_pulsar.consumer(
        #        PULSAR_IP, PULSAR_PORT, PULSAR_TOPIC, "Pulsar"))
        #    p8.start()
        #    process_list.append(p8)

    for process in process_list:
        process.join()
