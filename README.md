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

- [Art Blocks](./datasets/art-blocks/data)
- [GitHub](./datasets/github/data)
- [MovieLens](./datasets/movielens/data)
- [Amazon books](./datasets/amazon-books/data/)

## :fast_forward: How to start the streams?

Place yourself in root folder and run:

```
python3 start.py --platforms <PLATFORMS> --dataset <DATASET>
```

The argument `<PLATFORMS>` can be:
- `kafka`,
- `redpanda`,
- `rabbitmq` and/or
- `pulsar`.

The argument `<DATASET>` can be:
-  `github` ,
-  `art-blocks` ,
-  `movielens` or
-  `amazon-books`.

That script will start chosen streaming platforms in docker container, and you will see messages from chosen dataset being consumed.

You can then connect with Memgraph and stream the data into the database by running:
```
docker-compose up <DATASET>-memgraph
```

For example, if you choose Kafka as a streaming platform and art-blocks for your dataset, you should run:
```
python3 start.py --platforms kafka --dataset art-blocks
```

> If you are a Windows user and the upper command doesn't work, try replacing `python3` with `python`.

Next, in the new terminal window run:
```
docker-compose up art-blocks-memgraph
```

## :scroll: References

There's no documentation yet, but it's coming soon! Throw us a star to keep up with upcoming changes.
