import csv
import json
import time
from kafka import KafkaProducer
from kafka.errors import NoBrokersAvailable


def load_movies(file_path):
    movies_dict = {}
    with open(file_path, mode="r") as file:
        csvReader = csv.DictReader(file)
        for rows in csvReader:
            movieId = rows["movieId"]
            movies_dict[movieId] = {
                "movieId": movieId,
                "title": rows["title"],
                "genres": rows["genres"].split("|"),
            }
    return movies_dict


def get_kafka_producer(retries=5, delay=5):
    for _ in range(retries):
        try:
            producer = KafkaProducer(
                bootstrap_servers="kafka:9092",
                value_serializer=lambda v: json.dumps(v).encode("utf-8"),
            )
            return producer
        except NoBrokersAvailable:
            print("No brokers available. Retrying in {} seconds...".format(delay))
            time.sleep(delay)
    raise Exception("Failed to connect to Kafka after several retries")


def produce_messages(movies_file_path, ratings_file_path):
    producer = get_kafka_producer()

    movies_dict = load_movies(movies_file_path)

    with open(ratings_file_path, mode="r") as file:
        csvReader = csv.DictReader(file)
        for rows in csvReader:
            data = {
                "userId": rows["userId"],
                "movie": movies_dict[rows["movieId"]],
                "rating": rows["rating"],
                "timestamp": rows["timestamp"],
            }
            producer.send("movies", value=data)
            print(f"Produced message: {data}")
            time.sleep(1)  # Sleep for a second to simulate real-time data production

    producer.flush()
    producer.close()


if __name__ == "__main__":
    produce_messages("movies.csv", "ratings.csv")
