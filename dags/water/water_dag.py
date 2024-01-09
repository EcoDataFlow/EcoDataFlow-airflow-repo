from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.google.cloud.transfers.local_to_gcs import LocalFilesystemToGCSOperator

import requests
import pandas as pd

# 1. default
default_args = {
    "owner": "airflow",
    "start_date": datetime(2024, 1, 1),
    "retries": 1,
}

dag = DAG(
    "water_dag",
    default_args=default_args,
    catchup=False,
    schedule="@daily",  # TODO: 더 정교한 정의
    tags=["water"],
)


# 2. 정수장 정보 갖고 오기
# global water_plants_df
# water_plants_df = []
def get_water_purification_plants_info():
    url = "http://apis.data.go.kr/B500001/waterways/wdr/waterfltplt/waterfltpltlist"
    params = {
        "serviceKey": "1t9qpufmViYr8j1cg/mF7xDiJwF/hryABhn1HPIKmAby1X0JhKhjPjpmdgDpqiffQQdRHWa9iKBpBpcatnP79g==",
        "numOfRows": "100",
        "pageNo": "1",  # 응답 생긴 거 한번 본 담에 totalCount//numOfRows+1 하거나, 200 응답 오는 동안 try/while 하다 아니면 빠져나오게.
        "_type": "json",
    }

    global water_plants_df
    try:
        response = requests.get(url, params=params).json()["response"]["body"]
        jsons = response["items"]["item"]
        num_of_pages = response["totalCount"] // response["numOfRows"] + 1
        water_plants_df = pd.DataFrame(jsons)
        for i in range(2, num_of_pages+1):
            i_str = str(i)
            params["pageNo"] = i_str
            temp_df = pd.DataFrame(requests.get(url, params=params).json()["response"]["body"]["items"]["item"])
            water_plants_df.append(temp_df)
    except Exception as err:
        print(f"{err}")  # TODO: 로깅으로 바꿔서 커밋 올리던지..?
    return water_plants_df


# 3. fetch response jsons
def fetched_jsons_to_csv():
    fcodes = {"A002": "고령정수장", "A004": "고산정수장", "A005": "고양정수장", "A007": "공주정수장", "A012": "구미정수장(신)", "A013": "구천정수장",
              "A014": "군산정수장", "A015": "금산정수장", "A018": "대불정수장", "A020": "덕소정수장", }
    url = "http://apis.data.go.kr/B500001/waterways/wdr/dailindwater/dailindwaterlist"
    params = {
        "serviceKey": "1t9qpufmViYr8j1cg/mF7xDiJwF/hryABhn1HPIKmAby1X0JhKhjPjpmdgDpqiffQQdRHWa9iKBpBpcatnP79g==",
        "numOfRows": "100",
        # TODO: 변수화
        "pageNo": "1",  # 응답 생긴 거 한번 본 담에 totalCount/page 수? 하거나, 200응답 오는 동안 try/while 하다 아니면 빠져나오게.
        "_type": "json",
        "fcode": "",
        "stdt": "2023-12-01",
        "eddt": "2023-12-02",
    }

    for k, v in fcodes.items():
        params["fcode"] = k
        response = requests.get(url, params=params)
        jsons = response.json()["response"]["body"]["items"]
        df = pd.DataFrame(jsons)
        df.to_csv("dags/water/output.csv", index=False)


fetch_data_task = PythonOperator(
    task_id="fetch_response_jsons_task",
    python_callable=fetched_jsons_to_csv,
    dag=dag,
)

# 4. upload csv files to GCS
upload_operator = LocalFilesystemToGCSOperator(
    task_id="upload_csv_to_gcs_task",
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
