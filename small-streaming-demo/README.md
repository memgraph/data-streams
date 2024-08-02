## How to run:
1. Download the code attached
2. Run `docker compose up --build` (this will get Zookeeper, Kafka, producer script, Memgraph and Memgraph Lab running). The messages that are produced are from the MovieLens dataset. Messages are being produced to the `movies` topic. 
3. Once you see messages being produced in the terminal (logs from the producer), head over to `localhost:3000` (Memgraph Lab) and click on Connect. Run the following queries: 
// Create Kafka consumer which consumes messages from the movies topic and transforms them with the `movies.rating` transformation module
`CREATE KAFKA STREAM movies TOPICS movies TRANSFORM movies.rating BOOTSTRAP_SERVERS 'kafka:9092';`
// Start the stream
`START STREAM movies;`
4. If all is working correctly, you should see the growing number of nodes and edges and you can check the graph schema in Memgraph Lab to see what kind of dataset is created. 

## Important things to note:
- To set it all up in Docker Compose, I needed to put all services under the same network
- The Memgraph folder holds the transformations folder, which has the transformation module that tells Memgraph how to transform simple JSON messages from a Kafka topic into a Cypher query
- In Memgraph's Dockerfile, it is important to copy the transformations folder into the container so the transformation module is visible to Memgraph. Also, `--query-modules-directory` needed to be updated in the docker compose file to point to the transformations folder in order for this to work. 
- Here is the related documentation: https://memgraph.com/docs/data-streams 
