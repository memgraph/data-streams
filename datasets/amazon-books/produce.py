import csv
import stream.producer as producer

DATA = "data/books.csv"

def generate():
    while True:
        with open(DATA) as file:
            csvReader = csv.DictReader(file)
            for rows in csvReader:
                data = {
                    'bookId': rows['bookId'],
                    'userId': rows['userId'],
                    'rating': rows['rating'],
                    'timestamp': rows['timestamp'],
                    'title' : rows['title']
                }
                yield data

def main():
    producer.run(generate)


if __name__== "__main__":
    main()