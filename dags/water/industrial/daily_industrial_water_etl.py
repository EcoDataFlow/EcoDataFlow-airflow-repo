from airflow import DAG

from datetime import datetime  # , timedelta

# 로컬에선 ImportError 떠도 도커 에어플로 실행 환경에선 이렇게 해야만 에러가 없음
# from water.industrial.industrial_water_task_factory import IndustrialWaterTaskFactory

# pytest 통과해야 할 때는 이 경로로
from dags.water.industrial.industrial_water_task_factory import IndustrialWaterTaskFactory
# from industrial_water_task_factory import IndustrialWaterTaskFactory


default_args = {
    "owner": "airflow",
    "start_date": datetime(2024, 1, 1),
    "retries": 0,
    # "retry_delay": timedelta(seconds=5),
}

# start_date=airflow.utils.dates.days_ago(5),

with DAG(
    dag_id="new_daily_industrial_water_etl",
    default_args=default_args,
    catchup=False,
    schedule="@daily",
    tags=["daily_industrial_water_etl"],
) as dag:
    iwt = IndustrialWaterTaskFactory(
        "http://apis.data.go.kr/B500001/waterways/wdr/dailindwater/dailindwaterlist",
        "daily",
        {
            "item1": "temperature",
            "item3": "pH",
            "item5": "NTU",
            "item7": "electrical_conductivity",
            "item9": "alkalinity",
        },
        dag,
    )
    iwt.industrial_water_tasks_generator()
