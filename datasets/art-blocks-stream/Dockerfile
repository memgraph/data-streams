FROM python:3.8

# Install CMake for gqlalchemy
RUN apt-get update && \
    apt-get --yes install cmake && \
    rm -rf /var/lib/apt/lists/*

# Install packages
COPY /datasets/art-blocks-stream/requirements.txt ./
RUN pip3 install -r requirements.txt

COPY /datasets/art-blocks-stream/ /app/
COPY /stream /app/stream/
WORKDIR /app
