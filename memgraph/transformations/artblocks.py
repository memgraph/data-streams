import mgp
import json


@mgp.transformation
def sales(messages: mgp.Messages
          ) -> mgp.Record(query=str, parameters=mgp.Nullable[mgp.Map]):

    result_queries = []

    for i in range(messages.total_messages()):
        message = messages.message_at(i)
        sale_info = json.loads(message.payload().decode('utf8'))
        result_queries.append(
            mgp.Record(
                query=(
                    "CREATE (s:Sale {sale_id: $sale_id, payment_token: $payment_token, price: $price, datetime: $datetime})"
                    "MERGE (p:Project {project_id: $project_id})"
                    "CREATE (p)-[:HAS]->(s)"
                    "MERGE (a:Account {account_id: $seller_id})"
                    "CREATE (a)-[:IS_SELLING]->(s)"
                    "MERGE (b:Account {account_id: $buyer_id})"
                    "CREATE (b)-[:IS_BUYING]->(s)"
                    "MERGE (t:Token {token_id: $token_id})"
                    "CREATE (t)-[:IS_SOLD_IN]->(s)"),
                parameters={
                    "project_id": sale_info["project_id"],
                    "seller_id": sale_info["seller_id"],
                    "buyer_id": sale_info["buyer_id"],
                    "token_id": sale_info["token_id"],
                    "sale_id": sale_info["sale_id"],
                    "payment_token": sale_info["payment_token"],
                    "price": sale_info["price"],
                    "datetime": sale_info["datetime"]
                }))
    return result_queries
