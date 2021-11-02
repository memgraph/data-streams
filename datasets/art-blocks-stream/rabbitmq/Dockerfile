FROM python:3.8

# Install packages
COPY /datasets/art-blocks-stream/rabbitmq/requirements.txt ./
RUN pip3 install -r requirements.txt

COPY /datasets/art-blocks-stream/rabbitmq /app/
COPY /datasets/art-blocks-stream/data /app/
COPY /stream/rabbitmq/* /app/
WORKDIR /app

