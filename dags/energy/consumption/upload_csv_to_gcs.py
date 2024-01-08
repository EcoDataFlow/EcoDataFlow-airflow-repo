from datetime import datetime
from airflow import DAG
from airflow.providers.google.cloud.transfers.local_to_gcs import LocalFilesystemToGCSOperator

default_args = {
    "owner": "airflow",
    "start_date": datetime(2024, 1, 1),
    "retries": 1,
}
dag = DAG(
    "upload_csv_to_gcs",
    default_args=default_args,
    catchup=False,
    schedule="@daily",
)  # Or set your schedule
upload_operator = LocalFilesystemToGCSOperator(
    task_id="upload_csv_to_gcs_task",
    src="dags/data/region_codes_5digits.csv",
    dst="energy/consumption/region_codes_5digits.csv",
    bucket="data-lake-storage",
    gcp_conn_id="google_cloud_conn_id",  # The Conn Id from the Airflow connection setup
    dag=dag,
)
upload_operator