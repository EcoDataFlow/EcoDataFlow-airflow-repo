from datetime import datetime, timedelta
from airflow import DAG
from airflow.models import Variable
from airflow.operators.python_operator import PythonOperator
from airflow.providers.google.cloud.transfers.local_to_gcs import (
    LocalFilesystemToGCSOperator,
)
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import (
    GCSToBigQueryOperator,
)
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator
import requests
import pandas as pd
import os
import pendulum


def convert_date_format(date_str):
    datetime_obj = datetime.strptime(str(date_str), "%Y%m%d%H")
    return datetime_obj.strftime("%Y-%m-%d %H:%M")


def fetch_data_and_save_csv(**context):
    # Convert UTC to Korea Timezone
    utc_datetime = context["data_interval_end"]
    current_datetime = pendulum.instance(utc_datetime).in_tz("Asia/Seoul")
    current_date = current_datetime.strftime("%Y-%m-%d")
    current_time = current_datetime.strftime("%H")

    prev_datetime = current_datetime - timedelta(hours=1)
    prev_date = prev_datetime.strftime("%Y-%m-%d")
    prev_time = prev_datetime.strftime("%H")

    url = "http://apis.data.go.kr/B500001/rwis/waterQuality/list"
    params = {
        "serviceKey": "9IyndkiMrrzo5eLkP+I/sKhMYeg0jb8hNwqpdPHdeRKS5WuCsdT/bA8urOBesACx9E9cmdhLVs9sDvAFiyVlsA==",
        "_type": "json",
        "stDt": prev_date,
        "stTm": prev_time,
        "edDt": current_date,
        "edTm": current_time,
        "liIndDiv": "1",
        "numOfRows": "100",
        "pageNo": "1",
    }

    print(current_datetime)
    response = requests.get(url, params=params)
    json_data = response.json()["response"]["body"]["items"]["item"]
    df = pd.DataFrame(json_data)

    # select columns
    df_selected = df[
        [
            "occrrncDt",
            "fcltyAddr",
            "fcltyMngNm",
            "fcltyMngNo",
            "clVal",
            "phVal",
            "tbVal",
        ]
    ]
    # rename columns
    new_column_names = {
        "occrrncDt": "datetime",
        "fcltyAddr": "facultyAddr",
        "fcltyMngNm": "facultyName",
        "fcltyMngNo": "facultyCode",
        "clVal": "cl",
        "phVal": "pH",
        "tbVal": "tb",
    }

    df_selected.rename(columns=new_column_names, inplace=True)

    # change value ('점검중' 등 null로 변경해야 하는 값)
    for col_name in ["cl", "pH", "tb"]:
        df_selected.loc[df_selected[col_name] == "점검중", col_name] = ""

    df_selected["datetime"] = df_selected["datetime"].apply(convert_date_format)
    # convert to csv
    df_selected.to_csv("dags/water/output.csv", index=False)

    formatted_path = f"water/hourly/{current_datetime.strftime('%Y-%m-%d_%H')}.csv"
    Variable.set("gcs_file_path", formatted_path)


# Function to delete the CSV file
def delete_csv_file():
    file_path = "dags/water/output.csv"
    if os.path.exists(file_path):
        os.remove(file_path)
    else:
        print(f"The file {file_path} does not exist.")


default_args = {
    "owner": "airflow",
    "start_date": datetime(2024, 1, 1),
    "retries": 3,
    "retry_delay": timedelta(minutes=1),
}


dag = DAG(
    "hourly_water_pollutants_etl",
    default_args=default_args,
    catchup=False,
    schedule="20 * * * *",
    tags=["gcs"],
)

fetch_data_task = PythonOperator(
    task_id="fetch_data_and_save_csv_task",
    python_callable=fetch_data_and_save_csv,
    provide_context=True,
    dag=dag,
)


upload_operator = LocalFilesystemToGCSOperator(
    task_id="upload_csv_to_gcs_task",
    src="dags/water/output.csv",
    dst="{{ var.value.gcs_file_path }}",
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
    source_objects=["{{ var.value.gcs_file_path }}"],
    destination_project_dataset_table="focus-empire-410115.raw_data.hourly_water_pollutants_tmp",
    autodetect=True,
    write_disposition="WRITE_TRUNCATE",
    gcp_conn_id="google_cloud_conn_id",
    dag=dag,
)

create_table_if_not_exist = BigQueryInsertJobOperator(
    task_id="create_table_task",
    configuration={
        "query": {
            "query": """CREATE TABLE IF NOT EXISTS raw_data.hourly_water_pollutants
            AS SELECT * FROM raw_data.hourly_water_pollutants_tmp WHERE FALSE""",
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
            "query": """MERGE raw_data.hourly_water_pollutants A
            USING raw_data.hourly_water_pollutants_tmp T
            ON A.datetime = T.datetime and A.facultyCode = T.facultyCode
            WHEN NOT MATCHED THEN
                INSERT ROW""",
            "useLegacySql": False,
        }
    },
    gcp_conn_id="google_cloud_conn_id",
    dag=dag,
)


fetch_data_task >> upload_operator >> delete_file_task

upload_operator >> load_csv_to_bq_task

load_csv_to_bq_task >> create_table_if_not_exist >> execute_query_upsert
