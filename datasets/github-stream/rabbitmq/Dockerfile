FROM python:3.8

# Install packages
COPY /datasets/github-stream/rabbitmq/requirements.txt ./
RUN pip3 install -r requirements.txt

COPY /datasets/github-stream/rabbitmq /app/
COPY /datasets/github-stream/data /app/
COPY /stream/rabbitmq/* /app/
WORKDIR /app
