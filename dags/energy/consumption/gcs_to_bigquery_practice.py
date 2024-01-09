from datetime import datetime
from airflow import DAG
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator


default_args = {
    "owner": "airflow",
    "start_date": datetime(2024, 1, 1),
    "retries": 1,
}

dag = DAG(
    "electricity_consumption",
    default_args=default_args,
    catchup=False,
    schedule="@monthly",
)

gcs_to_bigquery = GCSToBigQueryOperator(
    task_id='gcs_to_bigquery',
    bucket="data-lake-storage",
    source_objects=["energy/consumption/electricity_consumption.csv"],
    destination_project_dataset_table="focus-empire-410115.raw_data.electricity_consumption",
    autodetect=True,
    write_disposition="WRITE_TRUNCATE",
    gcp_conn_id="google_cloud_conn_id",
    dag=dag,
)

gcs_to_bigquery
