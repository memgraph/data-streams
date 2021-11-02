FROM python:3.8

# Install packages
COPY /datasets/art-blocks-stream/kafka/requirements.txt ./
RUN pip3 install -r requirements.txt

COPY /datasets/art-blocks-stream/kafka /app/
COPY /datasets/art-blocks-stream/data /app/
COPY /stream/kafka/* /app/
WORKDIR /app
