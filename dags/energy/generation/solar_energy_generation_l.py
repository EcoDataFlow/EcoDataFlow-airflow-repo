import os
from datetime import datetime, timedelta
from airflow import DAG
from airflow.contrib.operators.file_to_gcs import LocalFilesystemToGCSOperator
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import (
    GCSToBigQueryOperator,
)
from airflow.operators.python_operator import PythonOperator
from airflow.operators.dagrun_operator import TriggerDagRunOperator

BUCKET_NAME = "data-lake-storage"


def get_formatted_execution_date(**kwargs):
    return (kwargs["execution_date"] - timedelta(days=29)).strftime("%Y%m%d")


def upload_to_gcs(**kwargs):
    csv_filename = "solar_energy_generation.csv"
    csv_filepath = os.path.join("dags/energy/generation/", csv_filename)

    upload_csv_to_gcs = LocalFilesystemToGCSOperator(
        task_id="upload_csv_to_gcs",
        src=csv_filepath,
        dst="energy/generation/" + f"{csv_filename}",
        bucket=BUCKET_NAME,
        gcp_conn_id="google_cloud_conn_id",
        dag=dag,
    )
    upload_csv_to_gcs.execute(context=kwargs)


def to_bigquery(**kwargs):
    csv_filename = "solar_energy_generation"
    gcs_to_bigquery = GCSToBigQueryOperator(
        task_id="gcs_to_bigquery",
        bucket=BUCKET_NAME,
        source_objects=["energy/generation/" + f"{csv_filename}.csv"],
        destination_project_dataset_table="focus-empire-410115.raw_data.solar_energy",
        source_format="CSV",
        autodetect=False,
        skip_leading_rows=1,
        schema_fields=[
            {"name": "hour", "type": "TIME"},
            {"name": "date", "type": "DATETIME"},
            {"name": "metro", "type": "STRING"},
            {"name": "amgo", "type": "FLOAT"},
            {"name": "region_code", "type": "INTEGER"},
        ],
        write_disposition="WRITE_APPEND",
        gcp_conn_id="google_cloud_conn_id",
        dag=dag,
    )
    gcs_to_bigquery.execute(context=kwargs)


default_args = {
    "owner": "airflow",
    "start_date": datetime(2023, 12, 15),
    "retries": 5,
    "retry_delay": timedelta(minutes=3),
}


dag = DAG(
    dag_id="api_gcs_to_bigquery",
    default_args=default_args,
    description="api_data_to_gcs",
    schedule_interval="@daily",
    catchup=False,
)


upload_csv_to_gcs_task = PythonOperator(
    task_id="upload_to_gcs",
    python_callable=upload_to_gcs,
    provide_context=True,
    dag=dag,
)


to_bigquery_task = PythonOperator(
    task_id="to_bigquery_task",
    python_callable=to_bigquery,
    provide_context=True,
    dag=dag,
)


trigger_target_dag_task = TriggerDagRunOperator(
    task_id="trigger_target_dag",
    trigger_dag_id="bigquery_elt_dag",  # 트리거하려는 대상 DAG의 ID
    dag=dag,
)


upload_csv_to_gcs_task >> to_bigquery_task >> trigger_target_dag_task
