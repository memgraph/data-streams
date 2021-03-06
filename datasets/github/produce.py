import ast
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
                    'followers': ast.literal_eval(rows['followers']),
                    'following': ast.literal_eval(rows['following']),
                }
                yield data


def main():
    producer.run(generate)


if __name__ == "__main__":
    main()
