import os
import logging
from datetime import timedelta

import pendulum
import pandas as pd
import requests
import pyarrow as pa
import pyarrow.parquet as pq

from airflow import DAG
from airflow.operators.python import PythonOperator

from google.cloud import storage

from airflow.providers.google.cloud.operators.bigquery import (
    BigQueryCreateExternalTableOperator,
    BigQueryDeleteTableOperator,
)

from airflow.providers.dbt.cloud.operators.dbt import (
    DbtCloudRunJobOperator,
)


# ============================================================
# Environment variables
# ============================================================

PROJECT_ID = os.environ.get("GCP_PROJECT_ID")

BUCKET = os.environ.get("GCP_GCS_BUCKET")

AIRFLOW_HOME = os.environ.get(
    "AIRFLOW_HOME",
    "/opt/airflow/"
)

BIGQUERY_DATASET = os.environ.get(
    "BIGQUERY_DATASET",
    "semiconductor_raw"
)


# ============================================================
# Constants
# ============================================================

TICKER = "^GSPC"

PARQUET_FILE = "SP500_data.parquet"

PARQUET_PATH = f"{AIRFLOW_HOME}/{PARQUET_FILE}"

GCS_OBJECT = f"sp500/{PARQUET_FILE}"

TABLE_NAME = "sp500_prices"


# ============================================================
# dbt Cloud configuration
# ============================================================

DBT_CLOUD_CONN_ID = "dbt_cloud_default"

DBT_JOB_ID = 70506183139547

DBT_ACCOUNT_ID = 70506183159993


# ============================================================
# Yahoo Finance API -> Parquet
# ============================================================

def format_to_parquet():

    logging.info(
        f"Downloading {TICKER} data from Yahoo Finance"
    )

    url = (
        "https://query1.finance.yahoo.com/"
        "v8/finance/chart/%5EGSPC"
    )

    params = {
        "period1": 0,
        "period2": int(
            pd.Timestamp.now().timestamp()
        ),
        "interval": "1d"
    }

    headers = {
        "User-Agent": "Mozilla/5.0"
    }

    response = requests.get(
        url,
        params=params,
        headers=headers,
        timeout=30
    )

    response.raise_for_status()

    data = response.json()

    result = data["chart"]["result"][0]

    quote = result["indicators"]["quote"][0]

    adj_close = (
        result["indicators"]["adjclose"][0]["adjclose"]
    )


    # ========================================================
    # JSON arrays -> DataFrame
    # ========================================================

    df = pd.DataFrame({
        "date": pd.to_datetime(
            result["timestamp"],
            unit="s"
        ),
        "open": quote["open"],
        "high": quote["high"],
        "low": quote["low"],
        "close": quote["close"],
        "adj_close": adj_close,
        "volume": quote["volume"]
    })

    logging.info(
        f"Downloaded {len(df)} rows"
    )

    logging.info(
        f"Columns: {df.columns.tolist()}"
    )

    logging.info(
        f"Data types:\n{df.dtypes}"
    )


    # ========================================================
    # DataFrame -> PyArrow Table
    # ========================================================

    table = pa.Table.from_pandas(
        df,
        preserve_index=False
    )


    # ========================================================
    # Write Parquet
    # ========================================================

    pq.write_table(
        table,
        PARQUET_PATH,
        compression="snappy"
    )

    logging.info(
        f"Parquet file created: {PARQUET_PATH}"
    )


# ============================================================
# Upload Parquet -> GCS
# ============================================================

def upload_to_gcs():

    logging.info(
        f"Uploading {PARQUET_FILE} to GCS"
    )

    client = storage.Client(
        project=PROJECT_ID
    )

    bucket = client.bucket(BUCKET)

    blob = bucket.blob(GCS_OBJECT)

    blob.upload_from_filename(
        PARQUET_PATH
    )

    logging.info(
        f"Upload successful: "
        f"gs://{BUCKET}/{GCS_OBJECT}"
    )


# ============================================================
# DAG configuration
# ============================================================

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}


# ============================================================
# DAG
# ============================================================

with DAG(
    dag_id="sp500_market_data",

    default_args=default_args,

    description=(
        "Ingest S&P 500 market data from Yahoo Finance "
        "to GCS, BigQuery External Table and dbt Cloud"
    ),

    start_date=pendulum.datetime(
        2026,
        8,
        1,
        tz="Europe/Amsterdam"
    ),

    schedule="0 8 * * *",

    catchup=False,

    tags=[
        "yahoo",
        "finance",
        "sp500"
    ],

) as dag:


    # ========================================================
    # Task 1
    # Yahoo Finance API -> Parquet
    # ========================================================

    download_sp500_data = PythonOperator(
        task_id="download_sp500_data",

        python_callable=format_to_parquet,
    )


    # ========================================================
    # Task 2
    # Parquet -> GCS
    # ========================================================

    upload_sp500_to_gcs = PythonOperator(
        task_id="upload_to_gcs",

        python_callable=upload_to_gcs,
    )


    # ========================================================
    # Task 3
    # Delete existing BigQuery External Table
    # ========================================================

    delete_external_table = BigQueryDeleteTableOperator(
        task_id="delete_existing_external_table",

        deletion_dataset_table=(
            f"{PROJECT_ID}."
            f"{BIGQUERY_DATASET}."
            f"{TABLE_NAME}"
        ),

        ignore_if_missing=True,

        gcp_conn_id="google_cloud_default",
    )


    # ========================================================
    # Task 4
    # GCS -> BigQuery External Table
    # ========================================================

    create_external_table = (
        BigQueryCreateExternalTableOperator(

            task_id="create_external_table",

            table_resource={

                "tableReference": {
                    "projectId": PROJECT_ID,
                    "datasetId": BIGQUERY_DATASET,
                    "tableId": TABLE_NAME,
                },

                "externalDataConfiguration": {

                    "sourceFormat": "PARQUET",

                    "sourceUris": [
                        f"gs://{BUCKET}/{GCS_OBJECT}"
                    ],

                    "autodetect": True,
                },
            },

            gcp_conn_id="google_cloud_default",
        )
    )


    # ========================================================
    # Task 5
    # Trigger dbt Cloud
    # ========================================================

    trigger_dbt_cloud = DbtCloudRunJobOperator(
        task_id="trigger_dbt_cloud",

        dbt_cloud_conn_id=DBT_CLOUD_CONN_ID,

        job_id=DBT_JOB_ID,

        account_id=DBT_ACCOUNT_ID,

        wait_for_termination=True,

        check_interval=30,

        timeout=1800,
    )


    # ========================================================
    # Task dependencies
    # ========================================================

    (
        download_sp500_data
        >> upload_sp500_to_gcs
        >> delete_external_table
        >> create_external_table
        >> trigger_dbt_cloud
    )

