from datetime import datetime
from airflow import DAG
from airflow.providers.google.cloud.transfers.gcs_to_local import GCSToLocalFilesystemOperator

default_args = {
    "owner": "airflow",
    "start_date": datetime(2024, 1, 1),
    "retries": 1,
}

dag = DAG(
    "download_csv_from_gcs",
    default_args=default_args,
    catchup=False,
    schedule="@monthly",
)  

download_file = GCSToLocalFilesystemOperator(
    task_id="download_file",
    object_name="energy/consumption/region_codes_5digits.csv",
    bucket="data-lake-storage",
    filename="dags/data/region_codes_5digits.csv",
    gcp_conn_id="google_cloud_conn_id",
    dag=dag,
)

download_file