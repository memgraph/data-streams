FROM python:3.8

# Install packages
COPY /datasets/github-stream/requirements.txt ./
RUN pip3 install -r requirements.txt

COPY /datasets/github-stream/ /app/
COPY /stream/kafka-redpanda/* /app/
COPY /stream/rabbitmq/* /app/
COPY /stream/pulsar/* /app/
WORKDIR /app
