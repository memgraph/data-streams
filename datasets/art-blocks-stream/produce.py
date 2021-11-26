import csv
import stream.producer as producer

DATA = "data/sales.csv"


def generate():
    while True:
        with open(DATA) as file:
            for line in file.readlines():
                line_list = line.strip().split(",")
                line_json = {
                    'project_id': line_list[0],
                    'sale_id': line_list[1],
                    'token_id': line_list[2],
                    'seller_id': line_list[3],
                    'buyer_id': line_list[4],
                    'payment_token': line_list[5],
                    'price': line_list[6],
                    'block_number': line_list[7],
                    'datetime': line_list[9]
                }
                yield line_json


def main():
    producer.run(generate)


if __name__ == "__main__":
    main()
