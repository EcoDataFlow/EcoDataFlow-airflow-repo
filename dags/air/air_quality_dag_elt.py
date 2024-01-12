from datetime import datetime
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator

import pandas as pd

default_args = {
    "owner": "airflow",
    "start_date": datetime(2024, 1, 1),
    "retries": 3,
}

dag = DAG(
    "air_quality_elt",
    default_args=default_args,
    catchup=False,
    schedule_interval=None,
    tags=["aqi"],
)

check_table_is_valid = DummyOperator(
    task_id="check_table_is_valid_task",
    dag=dag,
)


CTAS_total_aqi = BigQueryInsertJobOperator(
    task_id="CTAS_total_aqi_task",
    configuration={
        "query": {
            "query": """
            CREATE OR REPLACE TABLE analytics.total_air_quality_by_metro AS
            SELECT D.*, I.ISO_3166_2_CODE 
            FROM(
                SELECT *
                FROM `raw_data.air_quality`
                WHERE DATE(datetime) > DATE_SUB(CURRENT_DATE("Asia/Seoul"), INTERVAL 7 DAY)
            ) as D
            JOIN `raw_data.ISO_3166_2_KR` as I
            ON D.metro = I.metro
            ORDER BY datetime;
            """,
            "useLegacySql": False,
        }
    },
    gcp_conn_id="google_cloud_conn_id",
    dag=dag,
)

CTAS_current_aqi = BigQueryInsertJobOperator(
    task_id="CTAS_current_aqi_task",
    configuration={
        "query": {
            "query": """
            CREATE OR REPLACE TABLE analytics.current_air_quality_by_metro AS
            SELECT I.ISO_3166_2_CODE, D.*
            FROM (
                SELECT datetime, metro, AVG(so2) as avg_so2, AVG(co) as avg_co, AVG(o3) as avg_o3, AVG(no2) as avg_no2, AVG(pm10) as avg_pm10, AVG(pm25) as avg_pm25
                FROM raw_data.air_quality_tmp
                WHERE datetime IS NOT NULL
                GROUP BY metro, datetime
            ) as D
            JOIN `raw_data.ISO_3166_2_KR` as I
            ON D.metro = I.metro;
            """,
            "useLegacySql": False,
        }
    },
    gcp_conn_id="google_cloud_conn_id",
    dag=dag,
)

check_table_is_valid >> CTAS_total_aqi
check_table_is_valid >> CTAS_current_aqi
