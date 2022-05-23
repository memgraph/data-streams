from gqlalchemy import Memgraph
from pathlib import Path
from time import sleep
import logging
import os

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
    memgraph.execute("CREATE KAFKA STREAM ratings_stream TOPICS ratings TRANSFORM amazon_books.book_ratings BOOTSTRAP_SERVERS 'kafka:9092';")
    memgraph.execute("START STREAM ratings_stream;")

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
