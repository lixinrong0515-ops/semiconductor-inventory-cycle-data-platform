FROM apache/airflow:2.10.0

USER root

RUN apt-get update && \
    apt-get install -y curl gnupg && \
    rm -rf /var/lib/apt/lists/*

USER airflow

COPY requirements.txt .

RUN pip install --no-cache-dir \
    --constraint https://raw.githubusercontent.com/apache/airflow/constraints-2.10.0/constraints-3.12.txt \
    -r requirements.txt

COPY scripts/entrypoint.sh /opt/airflow/scripts/entrypoint.sh

USER root

RUN chmod +x /opt/airflow/scripts/entrypoint.sh

USER airflow

ENTRYPOINT ["/opt/airflow/scripts/entrypoint.sh"]