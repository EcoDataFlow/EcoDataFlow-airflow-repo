from datetime import datetime
from airflow import DAG
import pandas as pd
import requests
from airflow.operators.python_operator import PythonOperator
from airflow.providers.google.cloud.transfers.local_to_gcs import LocalFilesystemToGCSOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator


def process_data():
    # 데이터 처리 작업
    consumption = pd.read_csv("dags/data/electricity_consumption.csv")
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
    electricity_consumption_elt = pd.merge(iso_df, consumption, on='metro', how='right')

    # house_count와 power_use를 곱하여 total_usage 열을 생성
    electricity_consumption_elt['total_usage'] = (electricity_consumption_elt['house_count'] * electricity_consumption_elt['power_use']).round(2)

    cols = ['ISO_CODE'] + [col for col in electricity_consumption_elt.columns if col != 'ISO_CODE']
    electricity_consumption_elt = electricity_consumption_elt[cols]

    electricity_consumption_elt.to_csv('dags/data/electricity_consumption_elt.csv', index=False)


default_args = {
    "owner": "airflow",
    "start_date": datetime(2024, 1, 10),
    "retries": 1,
}

dag = DAG(
    "electricity_consumption_elt",
    default_args=default_args,
    catchup=False,
    schedule_interval=None,
)

process_data = PythonOperator(
    task_id='process_data',
    python_callable=process_data,
    dag=dag,
)

upload_to_gcs = LocalFilesystemToGCSOperator(
    task_id="upload_to_gcs",
    src="dags/data/electricity_consumption_elt.csv",
    dst="energy/consumption/electricity_consumption_elt.csv",
    bucket="data-lake-storage",
    gcp_conn_id="google_cloud_conn_id",
    dag=dag,
)

create_table_if_not_exist = BigQueryInsertJobOperator(
    task_id="create_table",
    configuration={
        "query": {
            "query": """
            CREATE TABLE IF NOT EXISTS analytics.electricity_consumption_elt (
                ISO STRING,
                metro STRING,
                city STRING,
                year_month STRING,
                house_count INT64,
                power_use FLOAT64,
                bill FLOAT64,
                total_usage FLOAT64
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
    source_objects="energy/consumption/electricity_consumption_elt.csv",
    destination_project_dataset_table="focus-empire-410115.analytics.electricity_consumption_elt",
    autodetect=True,
    write_disposition="WRITE_TRUNCATE",
    gcp_conn_id="google_cloud_conn_id",
    dag=dag,
)

process_data >> upload_to_gcs >> create_table_if_not_exist >> gcs_to_bigquery
