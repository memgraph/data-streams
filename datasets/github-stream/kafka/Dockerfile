FROM python:3.8

# Install packages
COPY /datasets/github-stream/kafka/requirements.txt ./
RUN pip3 install -r requirements.txt

COPY /datasets/github-stream/kafka /app/
COPY /datasets/github-stream/data /app/
COPY /stream/kafka/* /app/
WORKDIR /app
