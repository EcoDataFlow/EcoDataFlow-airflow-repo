import requests
import pandas as pd
import os
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.dagrun_operator import TriggerDagRunOperator

BUCKET_NAME = "data-lake-storage"


def get_formatted_execution_date(**kwargs):
    return (kwargs["execution_date"] - timedelta(days=29)).strftime("%Y%m%d")


def get_data(**kwargs):
    execution_date = get_formatted_execution_date(**kwargs)
    url = "http://apis.data.go.kr/B552115/PvAmountByLocHr/getPvAmountByLocHr"
    api_key = "d/SBgSmKAPxYCabQdjHocN4zvsxvdlL0w15/WgLq8DEjamKHBR7tdh0IbgNBsPvHfBBp+2LPyxtg6freIqxy1g=="

    params = {
        "serviceKey": api_key,
        "pageNo": 1,
        "numOfRows": 408,
        "dataType": "json",
        "tradeYmd": execution_date,
    }

    response = requests.get(url, params=params)
    items = response.json()["response"]["body"]["items"]["item"]
    csv_filename = "solar_energy_generation.csv"

    if not response.json():
        dag.pause()

    df = pd.DataFrame(items)
    df.to_csv("dags/energy/generation/" + csv_filename, index=False)


def csv_transform_region_code(**kwargs):
    # 현재 실행되는 파일의 경로
    current_directory = os.path.dirname(os.path.abspath(__file__))
    file_name = "solar_energy_generation.csv"
    file_path = os.path.join(current_directory, file_name)

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

    output_path = os.path.join(current_directory, file_name)
    df.to_csv(output_path, index=False)


def csv_transform_datetime(**kwargs):
    current_directory = os.path.dirname(os.path.abspath(__file__))
    file_name = "solar_energy_generation.csv"
    file_path = os.path.join(current_directory, file_name)
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

    output_path = os.path.join(current_directory, file_name)
    df.to_csv(output_path, index=False)


default_args = {
    "owner": "airflow",
    "start_date": datetime(2023, 12, 15),
    "retries": 5,
    "retry_delay": timedelta(minutes=3),
}


dag = DAG(
    dag_id="solar_energy_generation_et",
    default_args=default_args,
    description="api_data_extract",
    schedule_interval="@daily",
    catchup=False,
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

trigger_target_dag_task = TriggerDagRunOperator(
    task_id="trigger_target_dag",
    trigger_dag_id="solar_energy_generation_l",  # 트리거하려는 대상 DAG의 ID
    dag=dag,
)


api_to_csv >> csv_transform_region_code_task >> csv_transform_datetime_task

csv_transform_datetime_task >> trigger_target_dag_task
