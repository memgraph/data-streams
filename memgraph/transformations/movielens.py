import mgp
import json


@mgp.transformation
def rating(messages: mgp.Messages
             ) -> mgp.Record(query=str, parameters=mgp.Nullable[mgp.Map]):
    result_queries = []

    for i in range(messages.total_messages()):
        message = messages.message_at(i)
        movie_dict = json.loads(message.payload().decode('utf8'))
        result_queries.append(
            mgp.Record(
                query=("MERGE (u:User {id: $userId}) "
                       "MERGE (m:Movie {id: $movieId, title: $title}) "
                       "WITH u, m "
                       "UNWIND $genres as genre "
                       "MERGE (m)-[:OF_GENRE]->(:Genre {name: genre}) "
                       "MERGE (u)-[r:RATED {rating: ToFloat($rating), timestamp: $timestamp}]->(m)"),
                parameters={
                    "userId": movie_dict["userId"],
                    "movieId": movie_dict["movie"]["movieId"],
                    "title": movie_dict["movie"]["title"],
                    "genres": movie_dict["movie"]["genres"],
                    "rating": movie_dict["rating"],
                    "timestamp": movie_dict["timestamp"]}))

    return result_queries
