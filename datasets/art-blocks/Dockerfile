FROM python:3.8

# Install packages
COPY /datasets/art-blocks/requirements.txt ./
RUN pip3 install -r requirements.txt

COPY /datasets/art-blocks/ /app/
COPY /stream /app/stream/
WORKDIR /app
