import csv
import stream.producer as producer

DATA = "data/github-network.csv"


def generate():
    while True:
        with open(DATA) as file:
            csvReader = csv.DictReader(file)
            for rows in csvReader:
                data = {
                    'commit': rows['commit'],
                    'author': rows['author'],
                    'followers': rows['followers'],
                    'following': rows['following'],
                }
                yield data


def main():
    producer.run(generate)


if __name__ == "__main__":
    main()
