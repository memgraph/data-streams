import logging
import os

from gqlalchemy import Memgraph
from time import sleep


log = logging.getLogger(__name__)

MEMGRAPH_IP = os.getenv('MEMGRAPH_IP', 'memgraph-mage')
MEMGRAPH_PORT = os.getenv('MEMGRAPH_PORT', '7687')


def connect_to_memgraph(memgraph_ip, memgraph_port):
    memgraph = Memgraph(host=memgraph_ip, port=int(memgraph_port))
    while(True):
        try:
            if (memgraph._get_cached_connection().is_active()):
                return memgraph
        except:
            log.info("Memgraph probably isn't running.")
            sleep(1)


def set_stream(memgraph):
    log.info("Creating stream connections on Memgraph")
    memgraph.execute("""CREATE KAFKA STREAM github_commits 
                        TOPICS github 
                        TRANSFORM github_commits.commit  
                        BOOTSTRAP_SERVERS '54.74.181.194:9093'
                        CREDENTIALS {'sasl.username':'public', 
                                     'sasl.password':'public', 
                                     'security.protocol':'SASL_PLAINTEXT', 
                                     'sasl.mechanism':'PLAIN'};""")
    memgraph.execute("START STREAM github_commits;")

    # TODO: What to do when a new object is created
    """
    log.info("Creating triggers on Memgraph")
    memgraph.execute(
        "CREATE TRIGGER...")
    """


def main():
    memgraph = connect_to_memgraph(MEMGRAPH_IP, MEMGRAPH_PORT)
    set_stream(memgraph)


if __name__ == "__main__":
    main()
