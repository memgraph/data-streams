import csv
import stream.producer as producer

DATA = "data/sales.csv"


def generate():
    with open(DATA) as file:
        csvReader = csv.DictReader(file)
        for rows in csvReader:
            sale_id = rows["sale_id"]
            data = {
                sale_id: rows
            }
            yield data


def main():
    producer.run(generate)


if __name__ == "__main__":
    main()
