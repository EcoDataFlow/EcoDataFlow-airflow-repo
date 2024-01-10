from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.google.cloud.transfers.local_to_gcs import LocalFilesystemToGCSOperator

import requests, csv
import pandas as pd


# 1. default
default_args = {
    "owner": "airflow",
    "start_date": datetime(2024, 1, 7),
    "retries": 1,
}

dag = DAG(
    "water_dag",
    default_args=default_args,
    catchup=False,
    schedule="@daily",  # TODO: 더 정교한 정의
    tags=["water"],
)


# 2. get response jsons
def industrial_water_quality_infos_to_csv():
    # 1) load 정수장 정보
    with open("/Users/wonkyungkim/Documents/pythondev/EcoDataFlow-airflow-repo/dags/water/water_plants.csv", 'r') as csvfile:
        reader = csv.reader(csvfile)  # Assuming the first row contains the column headers
        water_plants = {row[0]: row[1] for row in reader}

    # 2) 정수장별로 데이터 수집
    url = "http://apis.data.go.kr/B500001/waterways/wdr/dailindwater/dailindwaterlist"
    params = {
        "serviceKey": "1t9qpufmViYr8j1cg/mF7xDiJwF/hryABhn1HPIKmAby1X0JhKhjPjpmdgDpqiffQQdRHWa9iKBpBpcatnP79g==",
        "numOfRows": "100",
        "pageNo": "1",
        "_type": "json",
        "fcode": "",
        "stdt": "2023-12-01",
        "eddt": "2023-12-01",
    }

    for k, v in water_plants.items():
        params["fcode"] = k
        response = requests.get(url, params=params)
        jsons = response.json()["response"]["body"]["items"]
        df = pd.DataFrame(jsons)
        df.to_csv("dags/water/output.csv", index=False)


fetch_data_task = PythonOperator(
    task_id="industrial_water_quality_infos_to_csv_task",
    python_callable=industrial_water_quality_infos_to_csv,
    dag=dag,
)


# 4. upload csv files to GCS
upload_operator = LocalFilesystemToGCSOperator(
    task_id="upload_csv_to_gcs_task",
    src="dags/water/output.csv",
    dst="water/industrial/{{ ds }}.csv",
    bucket="data-lake-storage",
    gcp_conn_id="google_cloud_conn_id",  # The Conn Id from the Airflow connection setup
    dag=dag,
)

fetch_data_task >> upload_operator

# def delete_csv_file():
#     file_path = "dags/water/output.csv"
#     if os.path.exists(file_path):
#         os.remove(file_path)
#     else:
#         print(f"The file {file_path} does not exist.")

# delete_file_task = PythonOperator(
#     task_id="delete_csv_file_task",
#     python_callable=delete_csv_file,
#     dag=dag,
# )
# >> delete_file_task
