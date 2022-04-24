"""
Test documentation
"""
from datetime import datetime, timedelta

from airflow import DAG

from airflow.operators.python import PythonOperator
from airflow.models import Variable


def print_variable():
    flag = Variable.get('is_startml')
    print(flag)


with DAG(
    'task_11_r_baldaev',
    default_args={
        'depends_on_past': False,
        'email': ['airflow@example.com'],
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 1,
        'retry_delay': timedelta(minutes=5),  # timedelta из пакета datetime
    },
    description='Task 11 - Variable example',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2022, 1, 1),
    catchup=False,
    tags=['r.baldaev-1'],
) as dag:
        task = PythonOperator(
            task_id='print_variable',
            python_callable=print_variable,
        )
