from airflow.operators.python import PythonOperator  # 에러 시 python_operator로 변경 후 실행
from airflow.providers.google.cloud.transfers.local_to_gcs import (
    LocalFilesystemToGCSOperator,
)
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import (
    GCSToBigQueryOperator,
)
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator

import requests
import pendulum
# import os
import pandas as pd


class IndustrialWaterTaskFactory:
    def __init__(self, url, schedule_interval, drop_columns, dag):
        self.url = url
        self.schedule_interval = schedule_interval
        self.drop_columns = drop_columns
        self.dag = dag

        self.gcp_conn_id = "google_cloud_conn_id"  # The Conn Id from the Airflow connection setup
        self.bucket = "data-lake-storage"
        self.local_path = f"dags/data/industrial_water/{self.schedule_interval}.csv"
        self.industrial_water_quality = pd.DataFrame()

    def define_params(self, **context):
        utc_datetime = context["data_interval_end"]
        current_datetime = pendulum.instance(utc_datetime).in_tz("Asia/Seoul")

        params = {
            "serviceKey": "YOUR_SERVICE_KEY",
            "numOfRows": "500",
            "pageNo": "1",
            "_type": "json",
            "stdt": current_datetime.subtract(months=8).strftime('%Y-%m-%d'),
            "eddt": current_datetime.strftime('%Y-%m-%d'),
        }

        return params

    def get_industrial_water_quality_infos(self, **context):
        # 1. define parameters and load address informations
        params = self.define_params(**context)
        # water_plants_info = pd.read_csv("/Users/wonkyungkim/Documents/pythondev/EcoDataFlow-airflow-repo/dags/water/industrial/new_water_plant_addresses.csv")
        water_plants_info = pd.read_csv(
            "dags/data/industrial_water/new_water_plant_addresses.csv"
        )

        # 2. get response jsons then create full df
        for i in range(water_plants_info.shape[0]):
            address_series = water_plants_info.iloc[i]
            params["fcode"] = address_series["fltplt"]
            address_series = pd.DataFrame(address_series).transpose().values.tolist()[0]
            try:
                self.create_full_df(params, address_series)
            except Exception as err:
                print(f"{err}")
        if len(self.industrial_water_quality) >= 1:
            self.convert_df_to_csv()

    def define_initial_df(self, response):
        before = pd.DataFrame(response["items"]["item"])
        new_columns = before.columns.to_list()
        new_columns.extend(["fltplt", "fltpltnm", "address", "add_code"])
        self.industrial_water_quality = pd.DataFrame(before, columns=new_columns)

    def create_full_df(self, params, address_series):
        response = requests.get(self.url, params=params).json()["response"]["body"]

        if len(response["items"]) >= 1:  # 200 OK 받아도 내용물이 없는 경우가 있어 조건문으로 Exception 발생 방지
            self.define_initial_df(response)
            num_of_pages, response_df = response["totalCount"] // response["numOfRows"] + 1, []
            for j in range(1, num_of_pages + 1):
                if num_of_pages > 1:
                    params["pageNo"] = str(j)
                    response_df = pd.DataFrame(
                        requests.get(self.url, params=params).json()["response"]["body"][
                            "items"
                        ]["item"]
                    )
                params_df = self.industrial_water_quality if num_of_pages == 1 else response_df
                for k in range(params_df.shape[0]):
                    params_df.loc[k, "fltplt":"add_code"] = address_series
                if num_of_pages > 1:
                    self.industrial_water_quality.append(response_df)

    def convert_df_to_csv(self):
        # 1) delete several columns
        for i in range(2, 2*(len(self.drop_columns)+1), 2):
            self.industrial_water_quality.pop(f"item{i}")

        # 2) alter several column names
        self.industrial_water_quality.rename(
            columns=self.drop_columns,
                inplace=True,
        )

        # 3) convert 'mesurede' values to the desired string format
        self.industrial_water_quality['mesurede'] = pd.to_datetime(self.industrial_water_quality['mesurede'], format='%Y%m%d')
        self.industrial_water_quality['mesurede'] = self.industrial_water_quality['mesurede'].dt.strftime('%Y-%m-%d')

        # 4) finally create csv
        # self.industrial_water_quality.to_csv("/Users/wonkyungkim/Documents/pythondev/EcoDataFlow-airflow-repo/dags/data/industrial_water/output.csv", index=False)  # for local unit test
        self.industrial_water_quality.to_csv(
            self.local_path, index=False, encoding="utf-8"
        )

    def industrial_water_tasks_generator(self):
        fetch_data_task = PythonOperator(
            task_id="industrial_water_quality_info_csv_task",
            python_callable=self.get_industrial_water_quality_infos,
            provide_context=True,
            dag=self.dag,
        )

        upload_operator = LocalFilesystemToGCSOperator(
            task_id=f"upload_{self.schedule_interval}_industrial_water_info_csv_to_gcs_task",
            # src="/Users/wonkyungkim/Documents/pythondev/EcoDataFlow-airflow-repo/dags/data/industrial_water/output.csv",
            src=self.local_path,
            dst=f"water/industrial/{self.schedule_interval}.csv",
            bucket=self.bucket,
            gcp_conn_id=self.gcp_conn_id,
            dag=self.dag,
        )

        load_csv_to_bq_task = GCSToBigQueryOperator(
            task_id="gcs_to_bigquery_task",
            bucket=self.bucket,
            source_objects=[f"water/industrial/{self.schedule_interval}.csv"],
            destination_project_dataset_table=f"focus-empire-410115.raw_data.{self.schedule_interval}_industrial_water_tmp",
            autodetect=True,
            write_disposition="WRITE_TRUNCATE",
            gcp_conn_id=self.gcp_conn_id,
            dag=self.dag,
        )

        create_table_if_not_exist = BigQueryInsertJobOperator(
            task_id="create_table_task",
            configuration={
                "query": {
                    "query": f"""CREATE TABLE IF NOT EXISTS raw_data.{self.schedule_interval}_industrial_water_tmp
                    AS SELECT * FROM raw_data.{self.schedule_interval}_industrial_water_tmp WHERE FALSE""",
                    "useLegacySql": False,
                }
            },
            gcp_conn_id=self.gcp_conn_id,
            dag=self.dag,
        )

        execute_query_upsert = BigQueryInsertJobOperator(
            task_id="execute_query_upsert_task",
            configuration={
                "query": {
                    "query": f"""MERGE raw_data.{self.schedule_interval}_industrial_water_tmp A
                    USING raw_data.{self.schedule_interval}_industrial_water_tmp T
                    ON A.mesurede = T.mesurede and A.fltpltnm = T.fltpltnm
                    WHEN NOT MATCHED THEN
                        INSERT ROW""",
                    "useLegacySql": False,
                }
            },
            gcp_conn_id=self.gcp_conn_id,
            dag=self.dag,
        )

        fetch_data_task >> upload_operator  # >> delete_file_task

        upload_operator >> load_csv_to_bq_task

        load_csv_to_bq_task >> create_table_if_not_exist >> execute_query_upsert

        return fetch_data_task, execute_query_upsert