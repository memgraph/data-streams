import subprocess
import argparse
from time import sleep
import socket
import os
import re

KAFKA_PORT = os.getenv('KAFKA_PORT', '9092')
REDPANDA_PORT = os.getenv('REDPANDA_PORT', '29092')
RABBITMQ_PORT = os.getenv('RABBITMQ_PORT', '5672')
PULSAR_PORT = os.getenv('PULSAR_PORT', '6650')
ZOOKEEPER_PORT_FULL = os.getenv('KAFKA_CFG_ZOOKEEPER_CONNECT', 'zookeeper:2181')


def parse_arguments():
    parser = argparse.ArgumentParser()
    parser.add_argument("--platforms", nargs="+", choices=["kafka", "redpanda", "rabbitmq", "pulsar"],
                        default=["kafka", "redpanda", "rabbitmq", "pulsar"])
    parser.add_argument("--dataset", type=str,
                        choices=["art-blocks-stream", "github-stream"], default="art-blocks-stream")

    value = parser.parse_args()
    return value


def docker_build_run(platforms, dataset):
    # build all choosen platforms
    for platform in platforms:
        subprocess.call("docker-compose build " + platform, shell=True)

    # build dataset
    subprocess.call("docker-compose build " + dataset, shell=True)

    for platform in platforms:
        subprocess.call(
            "docker-compose up -d " + platform, shell=True)

    # env-file: KAFKA, REDPANDA, RABBITMQ, PULSAR - default False
    # adding -e KAFKA=True -e REDPANDA=True will change those env var

    env_var = ""
    for platform in platforms:
        env_var += " " + "-e " + platform.upper() + "=True"

    # TODO wait for platforms
    sleep(8)
    subprocess.call("docker-compose run" +
                    env_var + " " + dataset, shell=True)


def is_port_in_use():
    ZOOKEEPER_PORT = re.findall(r'\d+', ZOOKEEPER_PORT_FULL)[0]
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        is_zookeeper_port_used = s.connect_ex(
            ('localhost', int(ZOOKEEPER_PORT)))
        is_kafka_port_used = s.connect_ex(('localhost', int(KAFKA_PORT)))
        is_redpanda_port_used = s.connect_ex(('localhost', int(REDPANDA_PORT)))
        is_rabbitmq_port_used = s.connect_ex(('localhost', int(RABBITMQ_PORT)))
        is_pulsar_port_used = s.connect_ex(('localhost', int(PULSAR_PORT)))
        return is_kafka_port_used == 0 or is_redpanda_port_used == 0 or is_rabbitmq_port_used == 0 or is_pulsar_port_used == 0 or is_zookeeper_port_used == 0


def main():
    platforms = list()
    dataset = ""
    value = parse_arguments()
    platforms = value.platforms
    dataset = value.dataset

    subprocess.call("docker-compose rm -sf", shell=True)
    if not is_port_in_use():
        docker_build_run(platforms, dataset)
    else:
        print("Ports in use. Try stopping services on necessary ports and run the script again.")

    print(platforms)
    print(dataset)


if __name__ == "__main__":
    main()
