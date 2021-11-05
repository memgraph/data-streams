import subprocess
import argparse
from time import sleep


def parse_arguments():
    parser = argparse.ArgumentParser()
    parser.add_argument("--platforms", nargs="+", choices=["kafka", "redpanda", "rabbitmq", "pulsar"],
                        default=["kafka", "redpanda", "rabbitmq", "pulsar"])
    parser.add_argument("--dataset", type=str,
                        choices=["art-blocks-stream", "github-stream"], default="art-blocks-stream")

    value = parser.parse_args()
    return value


def docker_build_run(platforms, dataset):
    subprocess.call("docker-compose rm -sf", shell=True)
    # TODO check local machine for ports

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
    sleep(5)
    subprocess.call("docker-compose run" +
                    env_var + " " + dataset, shell=True)


def main():
    platforms = list()
    dataset = ""
    value = parse_arguments()
    platforms = value.platforms
    dataset = value.dataset

    docker_build_run(platforms, dataset)

    print(platforms)
    print(dataset)


if __name__ == "__main__":
    main()
