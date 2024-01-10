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

BUCKET_NAME = "data-lake-storage"


def get_data():
    today = datetime.now()
    get_date = today - timedelta(days=27)
    url = "http://apis.data.go.kr/B552115/PvAmountByLocHr/getPvAmountByLocHr"
    api_key = "d/SBgSmKAPxYCabQdjHocN4zvsxvdlL0w15/WgLq8DEjamKHBR7tdh0IbgNBsPvHfBBp+2LPyxtg6freIqxy1g=="
    csv_filename = "solar_energy_generation.csv"
    start_date = get_date.strftime("%Y%m%d")

    params = {
        "serviceKey": api_key,
        "pageNo": 1,
        "numOfRows": 408,
        "dataType": "json",
        "tradeYmd": start_date,
    }
    response = requests.get(url, params=params)
    items = response.json()["response"]["body"]["items"]["item"]
    df = pd.DataFrame(items)
    df.to_csv("dags/energy/generation/" + csv_filename, index=False)


def csv_transform_region_code():
    # 현재 실행되는 파일의 경로
    current_directory = os.path.dirname(os.path.abspath(__file__))
    file_path = os.path.join(current_directory, "solar_energy_generation.csv")

    df = pd.read_csv(file_path)

    region_to_code = {
        "강원도": 42000,
        "경기도": 41000,
        "경상남도": 48000,
        "경상북도": 47000,
        "광주시": 29000,
        "대구시": 27000,
        "대전시": 25000,
        "부산시": 26000,
        "서울시": 11000,
        "세종시": 36110,
        "울산시": 31000,
        "인천시": 28000,
        "전라남도": 46000,
        "전라북도": 45000,
        "제주도": 50000,
        "충청남도": 44000,
        "충청북도": 43000,
    }
    df["regionCode"] = df["regionNm"].map(region_to_code)
    df["regionCode"] = df["regionCode"].astype(int)

    metro = {
        "서울시": "서울특별시",
        "부산시": "부산광역시",
        "대구시": "대구광역시",
        "인천시": "인천광역시",
        "광주시": "광주광역시",
        "대전시": "대전광역시",
        "울산시": "울산광역시",
        "경기도": "경기도",
        "강원도": "강원특별자치도",
        "충청북도": "충청북도",
        "충청남도": "충청남도",
        "전라북도": "전라북도",
        "전라남도": "전라남도",
        "경상북도": "경상북도",
        "경상남도": "경상남도",
        "제주도": "제주특별자치도",
        "세종시": "세종특별자치시",
    }
    df["regionNm"] = df["regionNm"].map(metro)

    output_path = os.path.join(current_directory, "solar_energy_generation.csv")
    df.to_csv(output_path, index=False)


def csv_transform_datetime():
    current_directory = os.path.dirname(os.path.abspath(__file__))
    file_path = os.path.join(current_directory, "solar_energy_generation.csv")
    df = pd.read_csv(file_path)
    df = df.drop(columns=["rn"])

    df["tradeNo"] = df["tradeNo"].apply(lambda x: f"{x % 24:02d}:00:00")
    df["tradeNo"] = pd.to_datetime(df["tradeNo"]).dt.strftime("%H:%M:%S").astype(str)
    df["tradeYmd"] = pd.to_datetime(df["tradeYmd"], format="%Y%m%d").dt.strftime(
        "%Y-%m-%d"
    )
    df = df.rename(
        columns={
            "tradeNo": "hour",
            "tradeYmd": "date",
            "regionNm": "metro",
            "amgo": "amgo",
            "regionCode": "region_code",
        }
    )

    output_path = os.path.join(current_directory, "solar_energy_generation.csv")
    df.to_csv(output_path, index=False)


default_args = {
    "owner": "airflow",
    "start_date": datetime(2023, 10, 1),
    "retries": 5,
    "retry_delay": timedelta(minutes=3),
}


dag = DAG(
    "csv_to_gcs_to_bigquery",
    default_args=default_args,
    description="csv->gcs->bigquery",
    schedule_interval="@daily",
    catchup=False,
)


start_task = DummyOperator(
    task_id="start_task",
    dag=dag,
)


api_to_csv = PythonOperator(
    task_id="api_to_csv",
    python_callable=get_data,
    dag=dag,
)


csv_transform_region_code_task = PythonOperator(
    task_id="csv_transform_region_code_task",
    python_callable=csv_transform_region_code,
    dag=dag,
)


csv_transform_datetime_task = PythonOperator(
    task_id="csv_transform_datetime_task",
    python_callable=csv_transform_datetime,
    dag=dag,
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
    autodetect=False,
    skip_leading_rows=1,
    schema_fields=[
        {"name": "hour", "type": "TIME"},
        {"name": "date", "type": "DATETIME"},
        {"name": "metro", "type": "STRING"},
        {"name": "amgo", "type": "FLOAT"},
        {"name": "region_code", "type": "INTEGER"},
    ],
    write_disposition="WRITE_APPEND",
    gcp_conn_id="google_cloud_conn_id",
    dag=dag,
)

start_task >> api_to_csv >> csv_transform_region_code_task

csv_transform_region_code_task >> csv_transform_datetime_task >> upload_csv_to_gcs

upload_csv_to_gcs >> gcs_to_bigquery
