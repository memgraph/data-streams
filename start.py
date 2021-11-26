import subprocess
import argparse
from time import sleep
import socket
import os
import re

# TODO: kafka alone not working

KAFKA_PORT = os.getenv('KAFKA_PORT', '9092')
REDPANDA_PORT = os.getenv('REDPANDA_PORT', '29092')
RABBITMQ_PORT = os.getenv('RABBITMQ_PORT', '5672')
PULSAR_PORT = os.getenv('PULSAR_PORT', '6650')
ZOOKEEPER_PORT_FULL = os.getenv('KAFKA_CFG_ZOOKEEPER_CONNECT', 'zookeeper:2181')
ZOOKEEPER_PORT = re.findall(r'\d+', ZOOKEEPER_PORT_FULL)[0]


def parse_arguments():
    parser = argparse.ArgumentParser()
    parser.add_argument("--platforms", nargs="+", choices=["kafka", "redpanda", "rabbitmq", "pulsar"],
                        default=["kafka", "redpanda", "rabbitmq", "pulsar"])
    parser.add_argument("--dataset", type=str,
                        choices=["art-blocks", "github", "movielens"], default="movielens")

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
    # adding -e KAFKA=True -e REDPANDA=True will change those env vars

    list_of_ports = list()
    env_var = ""
    for platform in platforms:
        env_var += " " + "-e " + platform.upper() + "=True"
        list_of_ports.append(platform.upper() + "_PORT")

    # TODO: check if PULSAR is really running - not based on port
    sleep(8)

    retries = 30

    ports_not_used = True
    while retries > 0 and ports_not_used:
        ports_not_used = False
        for port in list_of_ports:
            print(globals()[port])
            test_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            if test_socket.connect_ex(('localhost', int(globals()[port]))) != 0:
                ports_not_used = True
                print("platform at port " +
                      globals()[port] + " has not started.")
            test_socket.close()
        retries -= 1
        sleep(1)
    # sleep(5)
    if not ports_not_used:
        subprocess.call("docker-compose run" +
                        env_var + " " + dataset, shell=True)

    if retries == 0:
        print("Streaming platforms are not running correctly.")


def is_port_in_use():
    all_ports = ["ZOOKEEPER_PORT", "KAFKA_PORT",
                 "REDPANDA_PORT", "RABBITMQ_PORT", "PULSAR_PORT"]

    for port in all_ports:
        test_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        if test_socket.connect_ex(('localhost', int(globals()[port]))) == 0:
            return True
        test_socket.close()
    return False


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
