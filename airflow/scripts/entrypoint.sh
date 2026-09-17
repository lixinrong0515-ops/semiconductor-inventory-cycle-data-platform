#!/usr/bin/env bash

set -e

export GOOGLE_APPLICATION_CREDENTIALS="${GOOGLE_APPLICATION_CREDENTIALS}"
export AIRFLOW_CONN_GOOGLE_CLOUD_DEFAULT="${AIRFLOW_CONN_GOOGLE_CLOUD_DEFAULT}"

airflow db migrate

airflow users create \
    --username admin \
    --password admin \
    --firstname Admin \
    --lastname User \
    --role Admin \
    --email admin@example.com || true

if [[ "$1" == "scheduler" ]]; then
    exec airflow scheduler
else
    exec airflow webserver
fi