from datetime import datetime, timedelta
from airflow import DAG
from airflow.models import Variable
from airflow.operators.python import PythonOperator
from airflow.providers.google.cloud.transfers.local_to_gcs import (
    LocalFilesystemToGCSOperator,
)
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import (
    GCSToBigQueryOperator,
)
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
import requests
import pandas as pd
import os
import pendulum


def convert_date_format(date_str):
    print(date_str)
    if date_str is None:
        return None
    date, time = date_str.split()
    if time == "24:00":
        datetime_obj = datetime.strptime(date, "%Y-%m-%d") + timedelta(days=1)
    else:
        datetime_obj = datetime.strptime(date_str, "%Y-%m-%d %H:%M")
    return datetime_obj


def fetch_data_and_save_csv(**context):
    utc_datetime = context["data_interval_end"]
    current_datetime = pendulum.instance(utc_datetime).in_tz("Asia/Seoul")
    print(current_datetime)
    formatted_path = f"air/{current_datetime.strftime('%Y-%m-%d_%H')}.csv"
    Variable.set("aqi_gcs_file_path", formatted_path)

    aqi_api_key = Variable.get("aqi_api_key")
    url = "http://apis.data.go.kr/B552584/ArpltnInforInqireSvc/getCtprvnRltmMesureDnsty"
    params = {
        "serviceKey": aqi_api_key,
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

    for col_name in ["so2", "co", "o3", "no2", "pm10", "pm25"]:
        df_selected.loc[df_selected[col_name] == "-", col_name] = ""

    df_selected["datetime"] = df_selected["datetime"].apply(convert_date_format)

    # convert to csv
    df_selected.to_csv(os.path.abspath("air_output.csv"), index=False)


# Function to delete the CSV file
def delete_csv_file():
    file_path = os.path.abspath("air_output.csv")
    if os.path.exists(file_path):
        os.remove(file_path)
    else:
        print(f"The file {file_path} does not exist.")


default_args = {
    "owner": "airflow",
    "start_date": datetime(2024, 1, 1),
    "retries": 3,
}


dag = DAG(
    "air_quality_etl",
    default_args=default_args,
    catchup=False,
    schedule="30 * * * *",
    tags=["aqi"],
)

fetch_data_task = PythonOperator(
    task_id="fetch_data_and_save_csv_task",
    python_callable=fetch_data_and_save_csv,
    dag=dag,
)

upload_operator = LocalFilesystemToGCSOperator(
    task_id="upload_csv_to_gcs_task",
    src=os.path.abspath("air_output.csv"),
    dst="{{ var.value.aqi_gcs_file_path }}",
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
    source_objects=["{{ var.value.aqi_gcs_file_path }}"],
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

trigger_target_dag = TriggerDagRunOperator(
    task_id="trigger_target_dag",
    trigger_dag_id="air_quality_elt",  # 트리거하려는 대상 DAG의 ID
    dag=dag,
)

fetch_data_task >> upload_operator >> delete_file_task

upload_operator >> load_csv_to_bq_task

load_csv_to_bq_task >> create_table_if_not_exist >> execute_query_upsert

execute_query_upsert >> trigger_target_dag
