FROM python:3.8

# Install packages
COPY /datasets/movielens/requirements.txt ./
RUN pip3 install -r requirements.txt

COPY /datasets/movielens/ /app/
COPY /stream /app/stream/
WORKDIR /app
