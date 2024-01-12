from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator


default_args = {
    "owner": "airflow",
    "start_date": datetime(2023, 12, 15),
    "retries": 5,
    "retry_delay": timedelta(minutes=3),
}


dag = DAG(
    dag_id="solar_energy_generation_elt",
    default_args=default_args,
    description="bigquery_to_bigquery",
    schedule_interval=None,
    catchup=False,
)


query = """
    DROP TABLE IF EXISTS analytics.solar_energy_with_isocode;
    CREATE TABLE analytics.solar_energy_with_isocode AS
    SELECT
      se.*,
      iso.ISO_3166_2_CODE as isocode
    FROM
      raw_data.solar_energy se
    JOIN
      raw_data.ISO_3166_2_KR iso ON se.metro = iso.metro;
"""


elt_solar_with_iso_table = BigQueryInsertJobOperator(
    task_id="elt_solar_with_iso_table",
    configuration={
        "query": {
            "query": query,
            "useLegacySql": False,
        }
    },
    gcp_conn_id="google_cloud_conn_id",
    dag=dag,
)


elt_solar_with_iso_table
