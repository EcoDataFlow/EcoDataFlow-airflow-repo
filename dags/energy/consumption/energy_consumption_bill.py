from datetime import datetime
from airflow import DAG
import pandas as pd
from io import StringIO
import xml.etree.ElementTree as ET
import requests
import pandas as pd
from airflow.providers.google.cloud.hooks.gcs import GCSHook
from airflow.operators.python_operator import PythonOperator
from airflow.providers.google.cloud.transfers.local_to_gcs import LocalFilesystemToGCSOperator


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
        for month in range(9, 10):
            for code in region_codes.법정동코드:
                params = {'serviceKey' : 'IfMicP9ax2V2RmsEiy8nE8UW0OuO4zyv/DINJE/x6H5FVPTFKAFjM5scKDPGlgu9m05/ygawZ9h3egOzpH7usw==', 'sigunguCode' : code, 'searchDate' : f'{year}{month:02d}' }
                response = requests.get(url, params = params)
                
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
    print(energy_consumption_bill)

    energy_consumption_bill.to_csv("dags/data/energy_consumption_bill.csv", index=False)
    return "dags/data/energy_consumption_bill.csv"


default_args = {
    "owner": "airflow",
    "start_date": datetime(2024, 1, 1),
    "retries": 1,
}

dag = DAG(
    "energy_consumption_bill",
    default_args=default_args,
    catchup=False,
    schedule="@monthly",
)  

read_csv_task = PythonOperator(
    task_id='read_csv_from_gcs',
    python_callable=read_csv_from_gcs,
    dag=dag,
)

process_data_task = PythonOperator(
    task_id='process_data',
    python_callable=process_data,
    provide_context=True,
    dag=dag,
)

upload_operator = LocalFilesystemToGCSOperator(
    task_id="upload_csv_to_gcs_task",
    src="dags/data/energy_consumption_bill.csv",
    dst="energy/consumption/energy_consumption_bill.csv",
    bucket="data-lake-storage",
    gcp_conn_id="google_cloud_conn_id",  # The Conn Id from the Airflow connection setup
    dag=dag,
)

read_csv_task >> process_data_task >> upload_operator