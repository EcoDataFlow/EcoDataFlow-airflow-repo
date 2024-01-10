from datetime import datetime
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.providers.google.cloud.transfers.local_to_gcs import (
    LocalFilesystemToGCSOperator,
)
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import (
    GCSToBigQueryOperator,
)
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator
import requests
import pandas as pd
import os
import time


def fetch_data_and_save_csv():
    url = "http://apis.data.go.kr/B552584/ArpltnInforInqireSvc/getCtprvnRltmMesureDnsty"
    params = {
        "serviceKey": "9IyndkiMrrzo5eLkP+I/sKhMYeg0jb8hNwqpdPHdeRKS5WuCsdT/bA8urOBesACx9E9cmdhLVs9sDvAFiyVlsA==",
        "returnType": "json",
        "numOfRows": "1000",
        "pageNo": "1",
        "sidoName": "전국",
        "ver": "1.5",
    }

    response = requests.get(url, params=params)
    json_data = response.json()["response"]["body"]["items"]

    df = pd.DataFrame(json_data)

    # select columns
    df_selected = df[
        [
            "dataTime",
            "sidoName",
            "stationName",
            "stationCode",
            "so2Value",
            "coValue",
            "o3Value",
            "no2Value",
            "pm10Value",
            "pm25Value",
        ]
    ]
    # rename columns

    new_column_names = {
        "dataTime": "datetime",
        "sidoName": "metro",
        "so2Value": "so2",
        "coValue": "co",
        "o3Value": "o3",
        "no2Value": "no2",
        "pm10Value": "pm10",
        "pm25Value": "pm25",
    }

    df_selected.rename(columns=new_column_names, inplace=True)

    # modify value to official name of city

    new_metro_values = {
        "서울": "서울특별시",
        "부산": "부산광역시",
        "대구": "대구광역시",
        "인천": "인천광역시",
        "광주": "광주광역시",
        "대전": "대전광역시",
        "울산": "울산광역시",
        "경기": "경기도",
        "강원": "강원특별자치도",
        "충북": "충청북도",
        "충남": "충청남도",
        "전북": "전라북도",
        "전남": "전라남도",
        "경북": "경상북도",
        "경남": "경상남도",
        "제주": "제주특별자치도",
        "세종": "세종특별자치시",
    }
    df_selected["metro"] = df_selected["metro"].map(new_metro_values)

    # convert to csv
    df_selected.to_csv("dags/air/output.csv", index=False)


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
    schedule="30 * * * *",
    tags=["gcs"],
)

fetch_data_task = PythonOperator(
    task_id="fetch_data_and_save_csv_task",
    python_callable=fetch_data_and_save_csv,
    dag=dag,
)

upload_operator = LocalFilesystemToGCSOperator(
    task_id="upload_csv_to_gcs_task",
    src="dags/air/output.csv",
    dst="air/{{ execution_date.strftime('%Y-%m-%d_%H') }}.csv",
    bucket="data-lake-storage",
    gcp_conn_id="google_cloud_conn_id",  # The Conn Id from the Airflow connection setup
    dag=dag,
)

delete_file_task = PythonOperator(
    task_id="delete_csv_file_task",
    python_callable=delete_csv_file,
    dag=dag,
)

load_csv_to_bq_task = GCSToBigQueryOperator(
    task_id="gcs_to_bigquery_task",
    bucket="data-lake-storage",
    source_objects=["air/{{ execution_date.strftime('%Y-%m-%d_%H') }}.csv"],
    destination_project_dataset_table="focus-empire-410115.raw_data.air_quality_tmp",
    autodetect=True,
    write_disposition="WRITE_TRUNCATE",
    gcp_conn_id="google_cloud_conn_id",
    dag=dag,
)

create_table_if_not_exist = BigQueryInsertJobOperator(
    task_id="create_table_task",
    configuration={
        "query": {
            "query": """CREATE TABLE IF NOT EXISTS raw_data.air_quality
            AS SELECT * FROM raw_data.air_quality_tmp WHERE FALSE""",
            "useLegacySql": False,
        }
    },
    gcp_conn_id="google_cloud_conn_id",
    dag=dag,
)

execute_query_upsert = BigQueryInsertJobOperator(
    task_id="execute_query_upsert_task",
    configuration={
        "query": {
            "query": """MERGE raw_data.air_quality A
            USING raw_data.air_quality_tmp T
            ON A.datetime = T.datetime and A.stationCode = T.stationCode
            WHEN NOT MATCHED THEN
                INSERT ROW""",
            "useLegacySql": False,
        }
    },
    gcp_conn_id="google_cloud_conn_id",
    dag=dag,
)


fetch_data_task >> upload_operator >> delete_file_task

upload_operator >> load_csv_to_bq_task

load_csv_to_bq_task >> create_table_if_not_exist >> execute_query_upsert
