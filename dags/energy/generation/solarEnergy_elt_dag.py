import requests
import pandas as pd
import os
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.contrib.operators.file_to_gcs import LocalFilesystemToGCSOperator
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import (
    GCSToBigQueryOperator,
)
from airflow.operators.python_operator import PythonOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator
from airflow.sensors.external_task_sensor import ExternalTaskSensor


default_args = {
    "owner": "airflow",
    "start_date": datetime(2023, 12, 15),
    "retries": 5,
    "retry_delay": timedelta(minutes=3),
}


dag = DAG(
    dag_id="bigquery_elt_dag",
    default_args=default_args,
    description="bigquery_to_bigquery",
    schedule_interval="@daily",
    catchup=False,
)


# sensor = ExternalTaskSensor(
#     task_id="wait_for_gcs_update",
#     external_dag_id="api_data_to_gcs",
#     external_task_id="gcs_to_bigquery",
#     start_date=datetime(2023, 12, 15),
#     execution_date_fn=lambda x: x,
#     mode="reschedule",
#     timeout=3600,
# )


query = """
    DROP TABLE IF EXISTS analytics.solar_energy_with_isocode;
    CREATE TABLE analytics.solar_energy_with_isocode AS
    SELECT
      se.*,
      iso.ISO_3166_2_CODE as isocode
    FROM
      raw_data.solar_energy se
    JOIN
      raw_data.ISO_3166_2_KR iso ON se.metro = iso.metro;
"""


elt_solar_with_iso_table = BigQueryInsertJobOperator(
    task_id="elt_solar_with_iso_table",
    configuration={
        "query": {
            "query": query,
            "useLegacySql": False,
        }
    },
    gcp_conn_id="google_cloud_conn_id",
    dag=dag,
)


elt_solar_with_iso_table
