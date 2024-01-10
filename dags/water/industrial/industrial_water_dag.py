from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.google.cloud.transfers.local_to_gcs import LocalFilesystemToGCSOperator

import requests, logging
import pandas as pd


# 1. default
default_args = {
    "owner": "airflow",
    "start_date": datetime(2024, 1, 7),
    "retries": 1,
}

dag = DAG(
    "water_dag",
    default_args=default_args,
    catchup=False,
    schedule="@daily",  # TODO: 더 정교한 정의
    tags=["water"],
)


# 2. get response jsons
def convert_df_to_csv(daily_industrial_water_qual):
    if len(daily_industrial_water_qual) >= 1:
        daily_industrial_water_qual.pop("item2")
        daily_industrial_water_qual.pop("item4")
        daily_industrial_water_qual.pop("item6")
        daily_industrial_water_qual.pop("item8")
        daily_industrial_water_qual.pop("item10")
        daily_industrial_water_qual.rename(columns={"item1":"temperature", "item3":"pH", "item5":"NTU", "item7":"electrical_conductivity", "item9":"alkalinity"}, inplace=True)
        # converted_schema_df = pd.DataFrame(daily_industrial_water_qual, columns=["temperature", "pH", "NTU", "electrical_conductivity", "alkalinity", "mesurede"])
        daily_industrial_water_qual.to_csv("/Users/wonkyungkim/Documents/pythondev/EcoDataFlow-airflow-repo/dags/data/industrial_water/output.csv", index=False)


def preprocess_then_append_to_initial_df(initial_df, operand_df):
    pass


def get_industrial_water_quality_infos():
    url = "http://apis.data.go.kr/B500001/waterways/wdr/dailindwater/dailindwaterlist"
    params = {
        "serviceKey": "1t9qpufmViYr8j1cg/mF7xDiJwF/hryABhn1HPIKmAby1X0JhKhjPjpmdgDpqiffQQdRHWa9iKBpBpcatnP79g==",
        "numOfRows": "100",
        "pageNo": "1",
        "_type": "json",
        "stdt": "2023-12-01",
        "eddt": "2023-12-02",
    }

    daily_industrial_water_qual = []
    water_plants_info = pd.read_csv("/Users/wonkyungkim/Documents/pythondev/EcoDataFlow-airflow-repo/dags/water/industrial/new_water_plant_addresses.csv")
    for i in range(0, water_plants_info.shape[0]):
        address_series = water_plants_info.iloc[i]
        params["fcode"] = address_series["fltplt"]

        try:
            response = requests.get(url, params=params).json()["response"]["body"]

            if len(response["items"]) >= 1:
                num_of_pages = response["totalCount"] // response["numOfRows"] + 1
                before = pd.DataFrame(response["items"]["item"])
                new_columns = before.columns.to_list()
                new_columns.extend(["fltplt", "fltpltnm","address","add_code"])
                daily_industrial_water_qual = pd.DataFrame(before, columns=new_columns)

                address_series = pd.DataFrame(address_series).transpose().values.tolist()[0]
                if num_of_pages==1:
                    for i in range(daily_industrial_water_qual.shape[0]):
                        daily_industrial_water_qual.loc[i, "fltplt":"add_code"] = address_series
                else:
                    for i in range(2, num_of_pages + 1):
                        params["pageNo"] = str(i)
                        response_df = pd.DataFrame(requests.get(url, params=params).json()["response"]["body"]["items"]["item"])
                        for i in range(0, response_df.shape[0]):
                            response_df.loc[i, "fltplt":"add_code"] = address_series
                        daily_industrial_water_qual.append(response_df)

        except Exception as err:
            print(f"{err}")

    convert_df_to_csv(daily_industrial_water_qual)
    # return daily_industrial_water_qual


fetch_data_task = PythonOperator(
    task_id="industrial_water_quality_info_csv_task",
    python_callable=get_industrial_water_quality_infos,
    dag=dag,
)


# 4. upload csv files to GCS
upload_operator = LocalFilesystemToGCSOperator(
    task_id="upload_daily_industrial_water_info_csv_to_gcs_task",
    src="dags/water/output.csv",
    dst="water/industrial/{{ ds }}.csv",
    bucket="data-lake-storage",
    gcp_conn_id="google_cloud_conn_id",  # The Conn Id from the Airflow connection setup
    dag=dag,
)

fetch_data_task >> upload_operator

# def delete_csv_file():
#     file_path = "dags/water/output.csv"
#     if os.path.exists(file_path):
#         os.remove(file_path)
#     else:
#         print(f"The file {file_path} does not exist.")

# delete_file_task = PythonOperator(
#     task_id="delete_csv_file_task",
#     python_callable=delete_csv_file,
#     dag=dag,
# )
# >> delete_file_task
