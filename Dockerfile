FROM python:3.8.10

RUN apt-get update
RUN apt-get install curl jq -y

RUN python -m pip install --upgrade pip
COPY requirements.txt /requirements.txt
RUN pip install -r /requirements.txt

COPY docker-entry.sh /docker-entry.sh
