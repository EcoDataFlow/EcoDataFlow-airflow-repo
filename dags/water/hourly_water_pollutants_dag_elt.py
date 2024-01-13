from datetime import datetime
from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator

import pandas as pd

default_args = {
    "owner": "airflow",
    "start_date": datetime(2024, 1, 1),
    "retries": 3,
}

dag = DAG(
    "hourly_water_pollutants_elt",
    default_args=default_args,
    catchup=False,
    schedule=None,
    tags=["water_pollutant"],
)

check_table_is_valid = EmptyOperator(
    task_id="check_table_is_valid_task",
    dag=dag,
)


CTAS_current_water_pol = BigQueryInsertJobOperator(
    task_id="CTAS_current_water_pol_task",
    configuration={
        "query": {
            "query": """
            CREATE OR REPLACE TABLE analytics.current_hourly_water_pollutants_by_metro AS
            SELECT I.ISO_3166_2_CODE, D.*
            FROM(
            SELECT datetime, SPLIT(facultyAddr, ' ')[OFFSET(0)] as metro,  facultyName, cl, pH, tb
            FROM `focus-empire-410115.raw_data.hourly_water_pollutants_tmp`
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

CTAS_total_water_pol = BigQueryInsertJobOperator(
    task_id="CTAS_total_water_pol_task",
    configuration={
        "query": {
            "query": """
            CREATE OR REPLACE TABLE analytics.total_hourly_water_pollutants_by_metro AS
            SELECT D.*, I.ISO_3166_2_CODE
            FROM(
                SELECT datetime, SPLIT(facultyAddr, ' ')[OFFSET(0)] as metro,  facultyName, cl, pH, tb
                FROM `raw_data.hourly_water_pollutants`
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

check_table_is_valid >> CTAS_current_water_pol
check_table_is_valid >> CTAS_total_water_pol
