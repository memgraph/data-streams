import mgp
import json

@mgp.transformation
def commit(messages: mgp.Messages 
            )-> mgp.Record(query=str, parameters=mgp.Nullable[mgp.Map]):
    result_queries = []

    for i in range(messages.total_messages()):
        message = messages.message_at(i)
        commit_dict = json.loads(message.payload().decode('utf8'))
        result_queries.append(
            mgp.Record(
                query=("MERGE (u1:User {username: $author}) "
                       "MERGE (c:Commit {id: $commit}) "
                       "MERGE (c)-[:CREATED_BY]->(u1) "
                       "WITH u1 "
                       "UNWIND $followers AS follower "
                       "MERGE (u2:User {username: follower}) "
                       "MERGE (u2)-[:FOLLOWS]->(u1) "
                       "WITH u1 "
                       "UNWIND $following AS follows "
                       "MERGE (u3:User {username: follows}) "
                       "MERGE (u3)<-[:FOLLOWS]-(u1) "),
                parameters={
                    "commit": commit_dict["commit"],
                    "author": commit_dict["author"],
                    "followers": commit_dict["followers"],
                    "following": commit_dict["following"]
                    }))

    return result_queries
