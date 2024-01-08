from datetime import datetime
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import (
    GCSToBigQueryOperator,
)

import requests
import pandas as pd
import os


default_args = {
    "owner": "airflow",
    "start_date": datetime(2024, 1, 1),
    "retries": 1,
}

dag = DAG(
    "gcs_to_bigquery",
    default_args=default_args,
    catchup=False,
    schedule="@daily",
    tags=["gcs"],
)

load_csv = GCSToBigQueryOperator(
    task_id="gcs_to_bigquery_task",
    bucket="data-lake-storage",
    source_objects=["air/2024-01-08.csv"],
    destination_project_dataset_table=f"raw_data.air_quality",
    autodetect=True,
    write_disposition="WRITE_TRUNCATE",
    gcp_conn_id="google_cloud_conn_id",
    dag=dag,
)

load_csv
