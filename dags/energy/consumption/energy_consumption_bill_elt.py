from datetime import datetime
from airflow import DAG
import pandas as pd
from io import StringIO
import xml.etree.ElementTree as ET
import requests
from airflow.providers.google.cloud.hooks.gcs import GCSHook
from airflow.operators.python_operator import PythonOperator
from airflow.providers.google.cloud.transfers.local_to_gcs import LocalFilesystemToGCSOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator


def process_data(**kwargs):

    # 앞서 etl로 로컬에 저장된 데이터 가져오기
    region_codes = pd.read_csv("dags/data/region_codes.csv")
    bill = pd.read_csv("dags/data/energy_consumption_bill.csv")

    # 데이터 처리 작업
    bill['district_code'] = bill['district_code'].astype(str)
    bill['year_month'] = bill['year_month'].astype(str)
    region_codes['법정동코드'] = region_codes['법정동코드'].astype(str)

    bill['year_month'] = bill['year_month'].astype(str).str[:4] + '-' + bill['year_month'].str[4:]
    energy_consumption_bill = pd.merge(region_codes, bill, left_on='법정동코드', right_on='district_code', how='right')
    energy_consumption_bill = energy_consumption_bill.drop('폐지여부', axis=1)

    # 법정동명을 첫 번째 공백에서 나누어 metro와 city 추출
    split_columns = energy_consumption_bill['법정동명'].str.split(' ', n=1, expand=True)
    energy_consumption_bill['metro'] = split_columns[0]
    energy_consumption_bill['city'] = split_columns[1]

    # 열 순서 변경을 위한 열 이름 리스트 생성
    column_order = ['법정동명', 'metro', 'city'] + [col for col in energy_consumption_bill.columns if col not in ['법정동명', 'metro', 'city']]

    # DataFrame의 열 순서 변경
    energy_consumption_bill = energy_consumption_bill[column_order]
    energy_consumption_bill.drop(['법정동명', '법정동코드'], axis=1, inplace=True)
    energy_consumption_bill = energy_consumption_bill[~energy_consumption_bill['district_code'].astype(str).str.endswith('000')]

    energy_consumption_bill.drop('district_code', axis=1, inplace=True)
    # ISO 코드 붙이기
    iso_mapping = {
        'metro': [
            '부산광역시', '세종특별자치시', '대구광역시', '대전광역시', '전라북도',
            '충청북도', '울산광역시', '제주특별자치도', '충청남도', '인천광역시',
            '서울특별시', '전라남도', '광주광역시', '경기도', '강원특별자치도',
            '강원도', '경상북도', '경상남도'
        ],
        'ISO_CODE': [
            'KR-26', 'KR-50', 'KR-27', 'KR-30', 'KR-45',
            'KR-43', 'KR-31', 'KR-49', 'KR-44', 'KR-28',
            'KR-11', 'KR-46', 'KR-29', 'KR-41', 'KR-42',
            'KR-42', 'KR-47', 'KR-48'
        ]
    }

    iso_df = pd.DataFrame(iso_mapping)
    energy_consumption_bill = pd.merge(iso_df, energy_consumption_bill, on='metro', how='right')

    cols = ['ISO_CODE'] + [col for col in energy_consumption_bill.columns if col != 'ISO_CODE']
    energy_consumption_bill = energy_consumption_bill[cols]

    # electricity, heat, water_cool, water_hot 열 중 하나라도 null 값이 있는 행을 제거
    energy_consumption_bill = energy_consumption_bill.dropna(subset=['electricity', 'heat', 'water_cool', 'water_hot'])

    print(energy_consumption_bill)
    energy_consumption_bill.to_csv("dags/data/energy_consumption_bill_elt.csv", index=False)
    return "dags/data/energy_consumption_bill_elt.csv"


default_args = {
    "owner": "airflow",
    "start_date": datetime(2024, 1, 2),
    "retries": 1,
}

dag = DAG(
    "energy_consumption_bill_elt",
    default_args=default_args,
    catchup=False,
    schedule="@monthly",
)

process_data = PythonOperator(
    task_id='process_data',
    python_callable=process_data,
    provide_context=True,
    dag=dag,
)

upload_to_gcs = LocalFilesystemToGCSOperator(
    task_id="upload_csv_to_gcs_task",
    src="dags/data/energy_consumption_bill_elt.csv",
    dst="energy/consumption/energy_consumption_bill_elt.csv",
    bucket="data-lake-storage",
    gcp_conn_id="google_cloud_conn_id",  # The Conn Id from the Airflow connection setup
    dag=dag,
)

create_table_if_not_exist = BigQueryInsertJobOperator(
    task_id="create_table",
    configuration={
        "query": {
            "query": """
            CREATE TABLE IF NOT EXISTS analytics.energy_consumption_bill_elt (
                ISO_CODE STRING,
                metro STRING,
                city STRING,
                year_month STRING,
                electricity INT64,
                heat INT64,
                water_cool INT64,
                water_hot INT64
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
    source_objects="energy/consumption/energy_consumption_bill_elt.csv",
    destination_project_dataset_table="focus-empire-410115.analytics.energy_consumption_bill_elt",
    write_disposition="WRITE_TRUNCATE",
    gcp_conn_id="google_cloud_conn_id",
    dag=dag,
)

process_data >> upload_to_gcs
upload_to_gcs >> create_table_if_not_exist >> gcs_to_bigquery
