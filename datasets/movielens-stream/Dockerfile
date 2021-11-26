FROM python:3.8

# Install packages
COPY /datasets/movielens-stream/requirements.txt ./
RUN pip3 install -r requirements.txt

COPY /datasets/movielens-stream/ /app/
COPY /stream /app/stream/
WORKDIR /app
