<h1 align="center"> :bar_chart: data-streams :bar_chart:</h1>
<p align="center"> Publicly available real-time data sets on Kafka, Redpanda, RabbitMQ & Apache Pulsar</p>

## :speech_balloon: About

This project serves as a starting point for analyzing real-time streaming data.
We have prepared a few cool datasets which can be streamed via Kafka, Redpanda,
RabbitMQ, and Apache Pulsar. Right now, you can clone/fork the repo and start
the service locally, but we will be adding publicly available clusters to which
you can just connect.

## :open_file_folder: Datasets

Currently available datasets:

- [Art Blocks](./datasets/art-blocks-stream)
- [GitHub](./datasets/github-stream)

## :fast_forward: How to start a stream?

### Kafka

To start a Kafka stream, run:

```
docker-compose rm
docker-compose build
docker-compose up <DATA_STREAM>
```

Where `<DATA_STREAM>` can be either `github-stream` or `art-blocks-stream`. A
producer and consumer will be started automatically.

## :scroll: References

There's no documentation yet, but it's coming soon! Throw us a star to keep up
with upcoming changes.
