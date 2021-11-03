import json
import csv
from datetime import datetime
import pandas as pd

PROJECTS_CSV = "projects.csv"
ACCOUNTS_CSV = "accounts.csv"
TOKENS_CSV = "tokens.csv"
SALES_CSV = "sales.csv"


def sort_sales():
    df = pd.read_csv(SALES_CSV)
    sorted_df = df.sort_values(by=["timestamp"], ascending=True)
    sorted_df.to_csv(SALES_CSV, index=False)


def main():
    with open('projects_and_sales.json') as f:
        data = json.load(f)

    projects = data["data"]["projects"]

    with open(PROJECTS_CSV, 'w') as projects_file:
        with open(ACCOUNTS_CSV, 'w') as accounts_file:
            with open(TOKENS_CSV, 'w') as tokens_file:
                with open(SALES_CSV, 'w') as sales_file:

                    projects_header = ['project_id', 'contract_id',
                                       'project_name', 'active', 'complete', 'locked', 'website']
                    accounts_header = ['project_id',
                                       'account_id', 'account_name']
                    tokens_header = ['project_id',
                                     'owner_id', 'token_id', 'created_at']
                    sales_header = ['project_id', 'sale_id', 'token_id', 'seller_id',
                                    'buyer_id', 'payment_token', 'price', 'block_number', 'timestamp', 'datetime']

                    projects_writer = csv.DictWriter(
                        projects_file, quoting=csv.QUOTE_ALL, fieldnames=projects_header)
                    accounts_writer = csv.DictWriter(
                        accounts_file, quoting=csv.QUOTE_ALL, fieldnames=accounts_header)
                    tokens_writer = csv.DictWriter(
                        tokens_file, quoting=csv.QUOTE_ALL, fieldnames=tokens_header)
                    sales_writer = csv.DictWriter(
                        sales_file, quoting=csv.QUOTE_ALL, fieldnames=sales_header)

                    projects_writer.writeheader()
                    accounts_writer.writeheader()
                    tokens_writer.writeheader()
                    sales_writer.writeheader()

                    for project in projects:

                        # all info for projects.csv
                        project_id = project["id"]
                        contract_id = project_id.split("-")[0]
                        project_name = project["name"]
                        active = project["active"]
                        complete = project["complete"]
                        locked = project["locked"]
                        website = project["website"]

                        # add row in projects.csv
                        projects_writer.writerow({
                            'project_id': project_id,
                            'contract_id': contract_id,
                            'project_name': project_name,
                            'active': active,
                            'complete': complete,
                            'locked': locked,
                            'website': website,
                        })

                        # all info for accounts.csv
                        account_id = project["artistAddress"]
                        account_name = project["artistName"]

                        # add row in accounts.csv
                        accounts_writer.writerow({
                            'project_id': project_id,
                            'account_id': account_id,
                            'account_name': account_name
                        })

                        tokens = project["tokens"]
                        for token in tokens:
                            # all info for tokens.csv
                            token_id = token["id"]
                            owner_id = token["owner"]["id"]
                            created_at = token["createdAt"]

                            # add row in tokens.csv
                            tokens_writer.writerow({
                                'project_id': project_id,
                                'owner_id': owner_id,
                                'token_id': token_id,
                                'created_at': created_at
                            })

                        sales = project["openSeaSaleLookupTables"]
                        for sale in sales:
                            # all info for sales.csv
                            sale_id = sale["openSeaSale"]["id"]
                            seller_id = sale["openSeaSale"]["seller"]
                            buyer_id = sale["openSeaSale"]["buyer"]
                            payment_token = sale["openSeaSale"]["paymentToken"]
                            price = sale["openSeaSale"]["price"]
                            timestamp = sale["openSeaSale"]["blockTimestamp"]
                            dt_object = datetime.fromtimestamp(int(timestamp))

                            # there is one token in each sale, and it's first in list of sales -> [0]
                            sold_token_id = sale["openSeaSale"]["openSeaSaleLookupTables"][0]["token"]["id"]
                            block_number = sale["openSeaSale"]["blockNumber"]

                            # add row in sales.csv
                            sales_writer.writerow({
                                'project_id': project_id,
                                'sale_id': sale_id,
                                'token_id': sold_token_id,
                                'seller_id': seller_id,
                                'buyer_id': buyer_id,
                                'payment_token': payment_token,
                                'price': price,
                                'block_number': block_number,
                                'timestamp': timestamp,
                                'datetime': dt_object
                            })

    sort_sales()


if __name__ == "__main__":
    main()
