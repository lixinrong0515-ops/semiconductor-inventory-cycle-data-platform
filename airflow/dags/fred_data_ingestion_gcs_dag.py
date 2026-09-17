import os
import logging
from datetime import timedelta

import pendulum
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

API_KEY = os.environ.get("FRED_API_KEY")


# ============================================================
# FRED series
# ============================================================

FRED_SERIES = {
    "new_orders": "A35SNO",
    "inventories": "A34HTI",
    "semiconductor_ppi": "PCU334413334413A",
}


# ============================================================
# dbt Cloud configuration
# ============================================================

DBT_CLOUD_CONN_ID = "dbt_cloud_default"

DBT_JOB_ID = 70506183139547

DBT_ACCOUNT_ID = 70506183159993


# ============================================================
# FRED API -> Parquet
# ============================================================

def format_to_parquet(series_name, series_id):

    logging.info(
        f"Downloading FRED series "
        f"{series_name} ({series_id})"
    )

    url = (
        "https://api.stlouisfed.org/"
        "fred/series/observations"
    )

    params = {
        "api_key": API_KEY,
        "series_id": series_id,
        "file_type": "json"
    }

    response = requests.get(
        url,
        params=params,
        timeout=30
    )

    response.raise_for_status()

    data = response.json()

    observations = data["observations"]

    parquet_file = f"{series_name}.parquet"

    parquet_path = (
        f"{AIRFLOW_HOME}/{parquet_file}"
    )

    table = pa.Table.from_pylist(
        observations
    )

    pq.write_table(
        table,
        parquet_path,
        compression="snappy"
    )

    logging.info(
        f"Downloaded {len(observations)} observations"
    )

    logging.info(
        f"Parquet file created: {parquet_path}"
    )


# ============================================================
# Upload Parquet -> GCS
# ============================================================

def upload_to_gcs(
    bucket_name,
    object_name,
    local_file,
):

    storage.blob._MAX_MULTIPART_SIZE = (
        5 * 1024 * 1024
    )

    storage.blob._DEFAULT_CHUNKSIZE = (
        5 * 1024 * 1024
    )

    logging.info(
        f"Uploading {local_file} "
        f"to gs://{bucket_name}/{object_name}"
    )

    client = storage.Client(
        project=PROJECT_ID
    )

    bucket = client.bucket(
        bucket_name
    )

    blob = bucket.blob(
        object_name
    )

    blob.upload_from_filename(
        local_file
    )

    logging.info(
        f"Upload successful: "
        f"gs://{bucket_name}/{object_name}"
    )


# ============================================================
# DAG configuration
# ============================================================

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}


# ============================================================
# DAG
# ============================================================

with DAG(
    dag_id="fred_semiconductor",

    default_args=default_args,

    description=(
        "Ingest semiconductor FRED data "
        "to GCS, BigQuery External Tables "
        "and dbt Cloud"
    ),

    start_date=pendulum.datetime(
        2026,
        1,
        1,
        tz="Europe/Amsterdam"
    ),

    schedule="0 8 5 * *",

    catchup=False,

    tags=[
        "fred",
        "semiconductor"
    ],

) as dag:


    # ========================================================
    # Store the final external table tasks
    # ========================================================

    external_table_tasks = []


    # ========================================================
    # Create tasks for each FRED series
    # ========================================================

    for series_name, series_id in FRED_SERIES.items():

        parquet_file = (
            f"{series_name}.parquet"
        )


        # ====================================================
        # 1. FRED API -> Parquet
        # ====================================================

        format_to_parquet_task = PythonOperator(

            task_id=(
                f"format_parquet_{series_name}"
            ),

            python_callable=format_to_parquet,

            op_kwargs={

                "series_name": series_name,

                "series_id": series_id,

            },
        )


        # ====================================================
        # 2. Parquet -> GCS
        # ====================================================

        local_to_gcs_task = PythonOperator(

            task_id=(
                f"upload_gcs_{series_name}"
            ),

            python_callable=upload_to_gcs,

            op_kwargs={

                "bucket_name": BUCKET,

                "object_name": (
                    f"raw/{parquet_file}"
                ),

                "local_file": (
                    f"{AIRFLOW_HOME}/{parquet_file}"
                ),

            },
        )


        # ====================================================
        # 3. Delete existing BigQuery External Table
        # ====================================================

        delete_external_table_task = (
            BigQueryDeleteTableOperator(

                task_id=(
                    f"delete_external_{series_name}"
                ),

                deletion_dataset_table=(
                    f"{PROJECT_ID}."
                    f"{BIGQUERY_DATASET}."
                    f"fred_{series_name}"
                ),

                ignore_if_missing=True,

                gcp_conn_id=(
                    "google_cloud_default"
                ),
            )
        )


        # ====================================================
        # 4. Create BigQuery External Table
        # ====================================================

        bigquery_external_table_task = (
            BigQueryCreateExternalTableOperator(

                task_id=(
                    f"bigquery_external_{series_name}"
                ),

                table_resource={

                    "tableReference": {

                        "projectId": PROJECT_ID,

                        "datasetId": (
                            BIGQUERY_DATASET
                        ),

                        "tableId": (
                            f"fred_{series_name}"
                        ),
                    },

                    "externalDataConfiguration": {

                        "sourceFormat": "PARQUET",

                        "sourceUris": [

                            (
                                f"gs://{BUCKET}/raw/"
                                f"{parquet_file}"
                            )

                        ],

                        "autodetect": True,
                    },
                },

                gcp_conn_id=(
                    "google_cloud_default"
                ),
            )
        )


        # ====================================================
        # Dependencies for this FRED series
        # ====================================================

        (
            format_to_parquet_task
            >> local_to_gcs_task
            >> delete_external_table_task
            >> bigquery_external_table_task
        )


        # Save final task
        external_table_tasks.append(
            bigquery_external_table_task
        )


    # ========================================================
    # Trigger dbt Cloud only after ALL FRED series are ready
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
    # All FRED External Tables -> dbt Cloud
    # ========================================================

    external_table_tasks >> trigger_dbt_cloud