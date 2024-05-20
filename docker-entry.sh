#!/bin/bash


# wait for airflow-db to be ready
until airflow db init; do
    echo "airflow-db not ready, sleeping 10 secs ..."
    sleep 10
done
echo "airflow-db is ready!"

## Set env vars
airflow variables set TMP_DIR "/tmp/"
airflow variables set DAGS_CUSTOM_PARAMS '{}'

## Set connections (only required for docker)
echo "Adding connections ..."

# aws
# Connection named `aws_default` already exists in Airflow when starts a new server.
# Should remove before set new params
airflow connections delete aws_default
airflow connections add \
    --conn-uri $AIRFLOW_CONN_AWS_URI \
    aws_default 

## Start scheduler
airflow scheduler
