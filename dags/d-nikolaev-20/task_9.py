from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta
import json

str_text = 'xcom test'


def task_8_push_func():
    return "Airflow tracks everything"


def task_8_pull_func(ti):
    print(ti.xcom_pull(key="return_value",
                       task_ids="push_task"))


# Default settings applied to all tasks
default_args = {
    'depends_on_past': False,
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),  # timedelta из пакета datetime
}

with DAG(
        'hw_8_d-nikolaev',
        start_date=datetime(2021, 1, 1),
        max_active_runs=2,
        schedule_interval=timedelta(minutes=30),
        default_args=default_args,
        catchup=False
) as dag:
    task_1 = PythonOperator(
        task_id='push_task',
        python_callable=task_8_push_func,
    )
    task_2 = PythonOperator(
        task_id='pull_task',
        python_callable=task_8_pull_func,
    )
    task_1 >> task_2
