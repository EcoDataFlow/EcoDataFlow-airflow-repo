from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator  # 에러 시 python_operator로 변경 후 실행
from airflow.models import Variable
from airflow.providers.google.cloud.transfers.local_to_gcs import LocalFilesystemToGCSOperator
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator

import requests, pendulum, os
import pandas as pd

# 1. default
default_args = {
    "owner": "airflow",
    "start_date": datetime(2024, 1, 1),
    "retries": 0,
    # "retry_delay": timedelta(seconds=5),
}

dag = DAG(
    "daily_industrial_water_etl",
    default_args=default_args,
    catchup=False,
    schedule="@daily",
    tags=["industrial_water_etl"],
)


# 2. get response jsons
def convert_df_to_csv(daily_industrial_water_qual):
    if len(daily_industrial_water_qual) >= 1:
        daily_industrial_water_qual.pop("item2")
        daily_industrial_water_qual.pop("item4")
        daily_industrial_water_qual.pop("item6")
        daily_industrial_water_qual.pop("item8")
        daily_industrial_water_qual.pop("item10")
        daily_industrial_water_qual.rename(
            columns={"item1": "temperature", "item3": "pH", "item5": "NTU", "item7": "electrical_conductivity",
                     "item9": "alkalinity"}, inplace=True)
        # daily_industrial_water_qual.to_csv("/Users/wonkyungkim/Documents/pythondev/EcoDataFlow-airflow-repo/dags/data/industrial_water/output.csv", index=False)  # for local unit test
        daily_industrial_water_qual.to_csv("dags/data/industrial_water/output.csv", index=False, encoding='utf-8')


# def preprocess_then_append_to_initial_df(initial_df, operand_df):
#     pass


def get_industrial_water_quality_infos(**context):
    utc_datetime = context["data_interval_end"]
    current_datetime = pendulum.instance(utc_datetime).in_tz("Asia/Seoul")
    formatted_path = f"water/daily_industrial/{current_datetime.strftime('%Y-%m-%d_%H')}.csv"
    Variable.set("diw_gcs_file_path", formatted_path)

    url = "http://apis.data.go.kr/B500001/waterways/wdr/dailindwater/dailindwaterlist"
    params = {
        "serviceKey": "1t9qpufmViYr8j1cg/mF7xDiJwF/hryABhn1HPIKmAby1X0JhKhjPjpmdgDpqiffQQdRHWa9iKBpBpcatnP79g==",
        "numOfRows": "500",
        "pageNo": "1",
        "_type": "json",
        "stdt": "2023-12-01",
        "eddt": "2023-12-03",
    }

    daily_industrial_water_qual = []
    # water_plants_info = pd.read_csv("/Users/wonkyungkim/Documents/pythondev/EcoDataFlow-airflow-repo/dags/water/industrial/new_water_plant_addresses.csv")
    water_plants_info = pd.read_csv("dags/data/industrial_water/new_water_plant_addresses.csv")
    for i in range(water_plants_info.shape[0]):
        address_series = water_plants_info.iloc[i]
        params["fcode"] = address_series["fltplt"]

        try:
            response = requests.get(url, params=params).json()["response"]["body"]

            if len(response["items"]) >= 1:
                num_of_pages = response["totalCount"] // response["numOfRows"] + 1
                before = pd.DataFrame(response["items"]["item"])
                new_columns = before.columns.to_list()
                new_columns.extend(["fltplt", "fltpltnm", "address", "add_code"])
                daily_industrial_water_qual = pd.DataFrame(before, columns=new_columns)

                address_series = pd.DataFrame(address_series).transpose().values.tolist()[0]
                if num_of_pages == 1:
                    for j in range(daily_industrial_water_qual.shape[0]):
                        daily_industrial_water_qual.loc[j, "fltplt":"add_code"] = address_series
                else:
                    for j in range(2, num_of_pages + 1):
                        params["pageNo"] = str(j)
                        response_df = pd.DataFrame(
                            requests.get(url, params=params).json()["response"]["body"]["items"]["item"])
                        for k in range(response_df.shape[0]):
                            response_df.loc[k, "fltplt":"add_code"] = address_series
                        daily_industrial_water_qual.append(response_df)

        except Exception as err:
            print(f"{err}")

    convert_df_to_csv(daily_industrial_water_qual)
    # return daily_industrial_water_qual


fetch_data_task = PythonOperator(
    task_id="industrial_water_quality_info_csv_task",
    python_callable=get_industrial_water_quality_infos,
    provide_context=True,
    dag=dag,
)

# 4. upload csv files to GCS
upload_operator = LocalFilesystemToGCSOperator(
    task_id="upload_daily_industrial_water_info_csv_to_gcs_task",
    # src="/Users/wonkyungkim/Documents/pythondev/EcoDataFlow-airflow-repo/dags/data/industrial_water/output.csv",
    src="dags/data/industrial_water/output.csv",
    dst="{{ var.value.diw_gcs_file_path }}",
    bucket="data-lake-storage",
    gcp_conn_id="google_cloud_conn_id",  # The Conn Id from the Airflow connection setup
    dag=dag,
)

# 5. delete csv file
# def delete_csv_file():
#     file_path = "dags/data/industrial_water/output.csv"
#     if os.path.exists(file_path):
#         os.remove(file_path)
#     else:
#         print(f"The file {file_path} does not exist.")
#
#
# delete_file_task = PythonOperator(
#     task_id="delete_csv_file_task",
#     python_callable=delete_csv_file,
#     dag=dag,
# )


load_csv_to_bq_task = GCSToBigQueryOperator(
    task_id="gcs_to_bigquery_task",
    bucket="data-lake-storage",
    source_objects=["{{ var.value.diw_gcs_file_path }}"],
    destination_project_dataset_table="focus-empire-410115.raw_data.daily_industrial_water_tmp",
    autodetect=True,
    write_disposition="WRITE_TRUNCATE",
    gcp_conn_id="google_cloud_conn_id",
    dag=dag,
)

create_table_if_not_exist = BigQueryInsertJobOperator(
    task_id="create_table_task",
    configuration={
        "query": {
            "query": """CREATE TABLE IF NOT EXISTS raw_data.daily_industrial_water_tmp
            AS SELECT * FROM raw_data.daily_industrial_water_tmp WHERE FALSE""",
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
            "query": """MERGE raw_data.daily_industrial_water_tmp A
            USING raw_data.daily_industrial_water_tmp T
            ON A.mesurede = T.mesurede and A.fltpltnm = T.fltpltnm
            WHEN NOT MATCHED THEN
                INSERT ROW""",
            "useLegacySql": False,
        }
    },
    gcp_conn_id="google_cloud_conn_id",
    dag=dag,
)

fetch_data_task >> upload_operator  # >> delete_file_task

upload_operator >> load_csv_to_bq_task

load_csv_to_bq_task >> create_table_if_not_exist >> execute_query_upsert
