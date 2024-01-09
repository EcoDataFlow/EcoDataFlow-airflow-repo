from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.contrib.operators.file_to_gcs import LocalFilesystemToGCSOperator
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator

BUCKET_NAME = "data-lake-storage"

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2023, 10, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'csv_to_gcs_to_bigquery',
    default_args=default_args,
    description='csv->gcs->bigquery',
    schedule_interval='@daily',  # Adjust as needed
    catchup=False,
)

start_task = DummyOperator(
    task_id='start_task', 
    dag=dag
    )

upload_csv_to_gcs = LocalFilesystemToGCSOperator(
    task_id="upload_csv_to_gcs",
    src="dags/energy/generation/solar_energy_generation.csv",
    dst="energy/generation/solar_energy_generation.csv",
    bucket=BUCKET_NAME,
    gcp_conn_id="google_cloud_conn_id",
    dag=dag,
)

gcs_to_bigquery = GCSToBigQueryOperator(
    task_id="gcs_to_bigquery",
    bucket=BUCKET_NAME,
    source_objects=["energy/generation/solar_energy_generation.csv"],
    destination_project_dataset_table="focus-empire-410115.raw_data.solar_energy",
    source_format="CSV",
    autodetect=True,
    skip_leading_rows=1,
    write_disposition="WRITE_TRUNCATE",
    gcp_conn_id="google_cloud_conn_id",
    dag=dag,
)

start_task >> upload_csv_to_gcs >> gcs_to_bigquery
