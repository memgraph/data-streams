import logging
from gqlalchemy import Memgraph
from kafka.admin import KafkaAdminClient, NewTopic
from kafka.errors import TopicAlreadyExistsError, NoBrokersAvailable
from time import sleep
from pathlib import Path

log = logging.getLogger(__name__)


def connect_to_memgraph(memgraph_ip, memgraph_port):
    memgraph = Memgraph(host=memgraph_ip, port=int(memgraph_port))
    while(True):
        try:
            if (memgraph._get_cached_connection().is_active()):
                return memgraph
        except:
            log.info("Memgraph probably isn't running.")
            sleep(1)


def get_admin_client(kafka_ip, kafka_port):
    retries = 30
    while True:
        try:
            admin_client = KafkaAdminClient(
                bootstrap_servers=kafka_ip + ':' + kafka_port,
                client_id="art-blocks-stream")
            return admin_client
        except NoBrokersAvailable:
            retries -= 1
            if not retries:
                raise
            log.info("Failed to connect to Kafka")
            sleep(1)


def load_artblocks_data(memgraph):
    path_projects = Path("/usr/lib/memgraph/import-data/projects.csv")
    path_accounts = Path("/usr/lib/memgraph/import-data/accounts.csv")
    path_tokens = Path("/usr/lib/memgraph/import-data/tokens.csv")

    log.info("Loading projects...")
    memgraph.execute(
        f"""LOAD CSV FROM "{path_projects}"
            WITH HEADER DELIMITER "," AS row
            CREATE (p:Project {{project_id: ToString(row.project_id), project_name: ToString(row.project_name), active: ToString(row.active), 
                    complete: ToString(row.complete), locked: ToString(row.locked), 
                    website: ToString(row.website)}})
                    MERGE (c:Contract {{contract_id: ToString(row.contract_id)}})
                    CREATE (p)-[:IS_ON]->(c);"""
    )

    memgraph.execute(f"""CREATE INDEX ON :Project(project_id);""")

    log.info("Loading tokens...")
    memgraph.execute(
        f"""
        LOAD CSV FROM "{path_tokens}"
        WITH HEADER DELIMITER "," AS row  
        CREATE (t:Token {{token_id: ToString(row.token_id), created_at: ToString(row.created_at)}}) 
        MERGE (p:Project {{project_id: ToString(row.project_id)}}) 
        CREATE (t)-[:IS_PART_OF]->(p) 
        MERGE (a:Account {{account_id: ToString(row.owner_id)}})  
        CREATE (a)-[:MINTS]->(t);
        """
    )

    memgraph.execute(f"""CREATE INDEX ON :Token(token_id);""")

    log.info("Loading accounts...")
    memgraph.execute(
        f"""
        LOAD CSV FROM "{path_accounts}"   
        WITH HEADER DELIMITER "," AS row  
        MATCH (p:Project) WHERE p.project_id = row.project_id  
        MERGE (a:Account {{account_id: ToString(row.account_id), account_name: ToString(row.account_name)}})  
        CREATE (a)-[:CREATED]->(p);"""
    )

    memgraph.execute(f"""CREATE INDEX ON :Account(account_id);""")


def load_data(memgraph):
    """Load data into the database."""
    log.info("Loading data into Memgraph.")
    try:
        memgraph.drop_database()
        load_artblocks_data(memgraph)
    except Exception as e:
        log.info("Data loading error.")


def run(memgraph, kafka_ip, kafka_port):
    admin_client = get_admin_client(kafka_ip, kafka_port)
    log.info("Connected to Kafka")

    topic_list = [
        NewTopic(
            name="sales",
            num_partitions=1,
            replication_factor=1), ]

    try:
        admin_client.create_topics(new_topics=topic_list, validate_only=False)
    except TopicAlreadyExistsError:
        pass
    log.info("Created topics")

    log.info("Creating stream connections on Memgraph")
    memgraph.execute(
        "CREATE KAFKA STREAM sales_stream TOPICS sales TRANSFORM artblocks.sales")
    memgraph.execute("START STREAM sales_stream")

    # TODO: What to do when a new object is created
    """
    log.info("Creating triggers on Memgraph")
    memgraph.execute(
        "CREATE TRIGGER created_trigger ON CREATE AFTER COMMIT EXECUTE CALL publisher.create(createdObjects)")
    """
