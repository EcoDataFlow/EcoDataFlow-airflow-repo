from datetime import datetime
import requests

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.google.cloud.transfers.local_to_gcs import LocalFilesystemToGCSOperator


default_args = {
    "owner": "airflow",
    "start_date": datetime(2024, 1, 1),
    "retries": 1,
}

params = {
            "serviceKey": "1t9qpufmViYr8j1cg%2FmF7xDiJwF%2FhryABhn1HPIKmAby1X0JhKhjPjpmdgDpqiffQQdRHWa9iKBpBpcatnP79g%3D%3D",
        }


dag = DAG(
    dag_id="greenhouse-gas-dag",
    default_args=default_args,
    catchup=False,
    schedule="@monthly",  # Or set your schedule
)


def get_greenhouse_gas_data():
    base_url = f"https://www.kdhc.co.kr:443/openapi-data/service/kdhcCarbon/carbon?serviceKey={serviceKey}&pageNo=1&numOfRows=10&startDate=201007&endDate=201009"
    query_params = '/'.join(list(params.values()))
    url = f"{base_url}/{query_params}"
    res = requests.get(url, params=params, verify=False)
    res_json = res.json()


upload_operator = LocalFilesystemToGCSOperator(
    task_id="upload_csv_to_gcs_task",
    src="dags/file.csv",
    dst="file.csv",
    bucket="data-lake-storage",
    gcp_conn_id="google_cloud_conn_id",  # The Conn Id from the Airflow connection setup
    dag=dag,
)


upload_operator