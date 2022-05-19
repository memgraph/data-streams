import mgp

@mgp.transformation
def book_ratings(context: mgp.Messages 
            )-> mgp.Record(query=str, parameters=mgp.Nullable[mgp.Map]):
    result_queries = []

    for i in range(messages.total_messages()):
        message = messages.message_at(i)
        books_dict = json.loads(message.payload().decode('utf8'))
        result_queries.append(
            mgp.Record(
                query=("MERGE (b:Book {id: $bookId}) "
                       "MERGE (u:User {id: $userId}) "
                       "WITH u, b "
                       "CREATE (u)-[r:RATED {rating: ToFloat($rating), timestamp: $timestamp}]->(b)"),
                parameters={
                    "bookId": books_dict["bookId"],
                    "userId": books_dict["userId"],
                    "rating": books_dict["rating"],
                    "timestamp": books_dict["timestamp"]}))

    return result_queries
