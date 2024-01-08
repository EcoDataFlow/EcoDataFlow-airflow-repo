from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator

# Python 함수 정의
def my_function():
    print("Hello World")

# DAG 설정
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 7),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# DAG 인스턴스 생성
dag = DAG(
    'hello_world_dag',
    default_args=default_args,
    description='A simple Hello World DAG',
    schedule_interval=timedelta(days=1),
)

# PythonOperator 설정
t1 = PythonOperator(
    task_id='print_hello',
    python_callable=my_function,
    dag=dag,
)

t1
