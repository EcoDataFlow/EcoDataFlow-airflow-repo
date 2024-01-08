from datetime import datetime
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.providers.google.cloud.transfers.local_to_gcs import (
    LocalFilesystemToGCSOperator,
)
import requests
import pandas as pd
import os


def fetch_data_and_save_csv():
    url = "http://apis.data.go.kr/B552584/ArpltnInforInqireSvc/getCtprvnRltmMesureDnsty"
    params = {
        "serviceKey": "9IyndkiMrrzo5eLkP+I/sKhMYeg0jb8hNwqpdPHdeRKS5WuCsdT/bA8urOBesACx9E9cmdhLVs9sDvAFiyVlsA==",
        "returnType": "json",
        "numOfRows": "1000",
        "pageNo": "1",
        "sidoName": "전국",
        "ver": "1.0",
    }

    response = requests.get(url, params=params)
    json_data = response.json()["response"]["body"]["items"]
    df = pd.DataFrame(json_data)
    df.to_csv("dags/air/output.csv", index=False)


# Function to delete the CSV file
def delete_csv_file():
    file_path = "dags/air/output.csv"
    if os.path.exists(file_path):
        os.remove(file_path)
    else:
        print(f"The file {file_path} does not exist.")


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

fetch_data_task = PythonOperator(
    task_id="fetch_data_and_save_csv_task",
    python_callable=fetch_data_and_save_csv,
    dag=dag,
)

upload_operator = LocalFilesystemToGCSOperator(
    task_id="upload_csv_to_gcs_task",
    src="dags/air/output.csv",
    dst="air/{{ ds }}.csv",
    bucket="data-lake-storage",
    gcp_conn_id="google_cloud_conn_id",  # The Conn Id from the Airflow connection setup
    dag=dag,
)

delete_file_task = PythonOperator(
    task_id="delete_csv_file_task",
    python_callable=delete_csv_file,
    dag=dag,
)

fetch_data_task >> upload_operator >> delete_file_task
