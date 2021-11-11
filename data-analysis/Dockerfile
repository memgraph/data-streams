FROM python:3.8

# Install packages
COPY /data-analysis/requirements.txt ./
RUN pip3 install -r requirements.txt

COPY /data-analysis/ /app/
WORKDIR /app