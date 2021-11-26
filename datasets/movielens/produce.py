import csv
import stream.producer as producer

DATA_RATINGS = "data/ratings.csv"
DATA_MOVIES = "data/movies.csv"
movies_dict = {}


def generate():
    while True:
        with open(DATA_RATINGS) as file:
            csvReader = csv.DictReader(file)
            for rows in csvReader:
                data = {
                    'userId': rows['userId'],
                    'movie': movies_dict[rows['movieId']],
                    'rating': rows['rating'],
                    'timestamp': rows['timestamp'],
                }
                yield data


def main():
    with open(DATA_MOVIES) as file:
        csvReader = csv.DictReader(file)
        for rows in csvReader:
            movieId = rows['movieId']
            movies_dict[movieId] = {
                'movieId': movieId,
                'title': rows['title'],
                'genres': rows['genres'].split('|')
            }
    producer.run(generate)


if __name__ == "__main__":
    main()
