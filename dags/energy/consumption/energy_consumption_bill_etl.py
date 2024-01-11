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
from airflow.operators.dagrun_operator import TriggerDagRunOperator


def read_csv_from_gcs():
    # GCS Hook 초기화
    gcs_hook = GCSHook(gcp_conn_id='google_cloud_conn_id')

    # GCS에서 파일 읽기
    bucket_name = 'data-lake-storage'
    object_name = 'energy/consumption/region_codes_5digits.csv'
    file_content = gcs_hook.download(bucket_name, object_name)

    # 바이트 데이터를 문자열로 디코딩
    string_data = StringIO(file_content.decode('utf-8'))

    # pandas를 사용하여 데이터프레임으로 읽기
    region_codes = pd.read_csv(string_data)

    # 데이터프레임 작업
    region_codes.to_csv("dags/data/region_codes.csv", index=False)
    return "dags/data/region_codes.csv"


def process_data(**kwargs):

    # XCom을 통해 데이터 가져오기
    file_path = kwargs['ti'].xcom_pull(task_ids='read_csv_from_gcs')
    region_codes = pd.read_csv(file_path)
    # 데이터 처리 작업
    energy_consumption_bill = pd.DataFrame(columns=['year_month', 'district_code', 'electricity', 'heat', 'water_cool', 'water_hot'])
    url = 'http://apis.data.go.kr/1611000/ApHusEnergyUseInfoOfferService/getSignguAvrgEnergyUseAmountInfoSearch'
    for year in range(2023, 2024):
        for month in range(1, 13):

            for code in region_codes.법정동코드:
                params = {'serviceKey': 'IfMicP9ax2V2RmsEiy8nE8UW0OuO4zyv/DINJE/x6H5FVPTFKAFjM5scKDPGlgu9m05/ygawZ9h3egOzpH7usw==', 'sigunguCode': code, 'searchDate': f'{year}{month:02d}'}

                try:
                    response = requests.get(url, params=params)
                    response.raise_for_status()  # 응답 상태 확인

                    if response.headers['Content-Type'] != 'application/xml':
                        print('year, month, code, 응답이 XML 형식이 아닙니다.')
                        continue  # XML이 아니면 건너뛰기

                    # XML 응답 파싱
                    root = ET.fromstring(response.content)
                    for item in root.findall('.//item'):
                        electricity = item.find('elect').text if item.find('elect') is not None else None
                        heat = item.find('heat').text if item.find('heat') is not None else None
                        water_cool = item.find('waterCool').text if item.find('waterCool') is not None else None
                        water_hot = item.find('waterHot').text if item.find('waterHot') is not None else None

                        # 임시 데이터프레임 생성 및 병합
                        temp_df = pd.DataFrame([{
                            'year_month': f'{year}{month:02d}',
                            'district_code': code,
                            'electricity': electricity,
                            'heat': heat,
                            'water_cool': water_cool,
                            'water_hot': water_hot
                        }])
                        energy_consumption_bill = pd.concat([energy_consumption_bill, temp_df], ignore_index=True)

                except requests.exceptions.HTTPError as http_err:
                    print(year, month, code, f'HTTP 에러 발생: {http_err}')  # HTTP 에러 출력
                except ET.ParseError as parse_err:
                    print(year, month, code, f'XML 파싱 에러 발생: {parse_err}')  # 파싱 에러 출력
                except Exception as err:
                    print(year, month, code, f'기타 에러 발생: {err}')  # 기타 예외 처리

            print("checkpoint", year, month)
    print(energy_consumption_bill)
    energy_consumption_bill.to_csv("dags/data/energy_consumption_bill.csv", index=False)
    return "dags/data/energy_consumption_bill.csv"


default_args = {
    "owner": "airflow",
    "start_date": datetime(2024, 1, 1),
    "retries": 2,
}

dag = DAG(
    "energy_consumption_bill_etl",
    default_args=default_args,
    catchup=False,
    schedule="@monthly",
)

read_csv = PythonOperator(
    task_id='read_csv_from_gcs',
    python_callable=read_csv_from_gcs,
    dag=dag,
)

process_data = PythonOperator(
    task_id='process_data',
    python_callable=process_data,
    provide_context=True,
    dag=dag,
)

upload_to_gcs = LocalFilesystemToGCSOperator(
    task_id="upload_csv_to_gcs_task",
    src="dags/data/energy_consumption_bill.csv",
    dst="energy/consumption/energy_consumption_bill.csv",
    bucket="data-lake-storage",
    gcp_conn_id="google_cloud_conn_id",  # The Conn Id from the Airflow connection setup
    dag=dag,
)

create_table_if_not_exist = BigQueryInsertJobOperator(
    task_id="create_table",
    configuration={
        "query": {
            "query": """
            CREATE TABLE IF NOT EXISTS raw_data.energy_consumption_bill (
                year_month STRING,
                district_code INT64,
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
    source_objects="energy/consumption/energy_consumption_bill.csv",
    destination_project_dataset_table="focus-empire-410115.raw_data.energy_consumption_bill",
    autodetect=True,
    write_disposition="WRITE_TRUNCATE",
    gcp_conn_id="google_cloud_conn_id",
    dag=dag,
)

trigger_target_dag = TriggerDagRunOperator(
    task_id="trigger_target_dag",
    trigger_dag_id="energy_consumption_bill_elt",  # 트리거하려는 대상 DAG의 ID
    dag=dag,
)

read_csv >> process_data >> upload_to_gcs
upload_to_gcs >> create_table_if_not_exist >> gcs_to_bigquery >> trigger_target_dag
