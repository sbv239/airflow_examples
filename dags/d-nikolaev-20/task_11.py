from airflow import DAG

from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta
import json




str_text = 'xcom test'


def task_11_func():
    from airflow.models import Variable

    is_startml = Variable.get("is_startml")
    print(is_startml)

default_args = {
    'depends_on_past': False,
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),  # timedelta из пакета datetime
}

with DAG(
        'hw_11_d-nikolaev',
        start_date=datetime(2021, 1, 1),
        max_active_runs=2,
        schedule_interval=timedelta(minutes=30),
        default_args=default_args,
        catchup=False
) as dag:
    task_1 = PythonOperator(
        task_id='task_1',
        python_callable=task_11_func,
    )

