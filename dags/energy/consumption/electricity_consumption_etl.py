from datetime import datetime
from airflow import DAG
import pandas as pd
import requests
from airflow.operators.python_operator import PythonOperator
from airflow.providers.google.cloud.transfers.local_to_gcs import LocalFilesystemToGCSOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator
from airflow.operators.dagrun_operator import TriggerDagRunOperator


def process_data():
    # 데이터 처리 작업
    electricity_consumption = pd.DataFrame(columns=['metro', 'city', 'year_month', 'house_count', 'power_use', 'bill'])

    metrocode = [11, 21, 22, 23, 24, 25, 26, 31, 32, 33, 34, 35, 36, 37, 38, 39, 41]

    # 현재 연도와 월 가져오기
    current_year = datetime.now().year
    current_month = datetime.now().month

    for year in range(2021, current_year + 1):
        for month in range(1, 13):
            if year == current_year and month > current_month:
                break

            for code in metrocode:
                url = f'https://bigdata.kepco.co.kr/openapi/v1/powerUsage/houseAve.do?year={year}&month={month:02d}&metroCd={code}&apiKey=693nx2eTP7S0KVI83Qlg2gFSLO17g3qhB152EW8R&returnType=json'
                response = requests.get(url)
                response.encoding = 'utf-8'  # 인코딩 설정

                json_response = response.json()
                if 'data' in json_response:
                    data = json_response['data']
                    for entry in data:
                        # 데이터프레임에 행 추가
                        new_row = {
                            'metro': entry['metro'],
                            'city': entry['city'],
                            'year_month': f"{entry['year']}-{entry['month']}",
                            'house_count': entry['houseCnt'],
                            'power_use': entry['powerUsage'],
                            'bill': entry['bill']
                        }
                        electricity_consumption = pd.concat([electricity_consumption, pd.DataFrame([new_row])], ignore_index=True)

    metro_2 = {
        "서울특별시": "서울특별시",
        "부산광역시": "부산광역시",
        "대구광역시": "대구광역시",
        "인천광역시": "인천광역시",
        "광주광역시": "광주광역시",
        "대전광역시": "대전광역시",
        "울산광역시": "울산광역시",
        "경기도": "경기도",
        "강원도": "강원특별자치도",
        "충청북도": "충청북도",
        "충청남도": "충청남도",
        "전라북도": "전라북도",
        "전라남도": "전라남도",
        "경상북도": "경상북도",
        "경상남도": "경상남도",
        "제주특별자치도": "제주특별자치도",
        "세종특별자치시": "세종특별자치시",
    }

    electricity_consumption['metro'] = electricity_consumption['metro'].map(metro_2)

    electricity_consumption.to_csv('dags/data/electricity_consumption.csv', index=False)


default_args = {
    "owner": "airflow",
    "start_date": datetime(2024, 1, 10),
    "retries": 1,
}

dag = DAG(
    "electricity_consumption_etl",
    default_args=default_args,
    catchup=False,
    schedule="@monthly",
)

process_data = PythonOperator(
    task_id='process_data',
    python_callable=process_data,
    dag=dag,
)

upload_to_gcs = LocalFilesystemToGCSOperator(
    task_id="upload_to_gcs",
    src="dags/data/electricity_consumption.csv",
    dst="energy/consumption/electricity_consumption.csv",
    bucket="data-lake-storage",
    gcp_conn_id="google_cloud_conn_id",
    dag=dag,
)

create_table_if_not_exist = BigQueryInsertJobOperator(
    task_id="create_table",
    configuration={
        "query": {
            "query": """
            CREATE TABLE IF NOT EXISTS raw_data.electricity_consumption (
                metro STRING,
                city STRING,
                year_month STRING,
                house_count INT64,
                power_use FLOAT64,
                bill FLOAT64
            )
            """,
            "useLegacySql": False,
        }
    },
    gcp_conn_id="google_cloud_conn_id",
    dag=dag,
)

gcs_to_bigquery = GCSToBigQueryOperator(
    task_id="gcs_to_bigquery",
    bucket="data-lake-storage",
    source_objects="energy/consumption/electricity_consumption.csv",
    destination_project_dataset_table="focus-empire-410115.raw_data.electricity_consumption",
    autodetect=True,
    write_disposition="WRITE_TRUNCATE",
    gcp_conn_id="google_cloud_conn_id",
    dag=dag,
)

trigger_target_dag = TriggerDagRunOperator(
    task_id="trigger_target_dag",
    trigger_dag_id="electricity_consumption_elt",  # 트리거하려는 대상 DAG의 ID
    dag=dag,
)

process_data >> upload_to_gcs >> create_table_if_not_exist >> gcs_to_bigquery
gcs_to_bigquery >> trigger_target_dag
