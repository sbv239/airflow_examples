from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

import requests
import json


def print_airflow():
    return "Airflow tracks everything"

def puller(ti):
    pull_value = ti.xcom_pull(
        key='return_value',
        task_ids="print_airflow"
    )
    print(pull_value)
    return pull_value

with DAG(
    'blabla_8',

    default_args={
        'depends_on_past': False,
        'email': ['airflow@example.com'],
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 1,
        'retry_delay': timedelta(minutes=5),
    },
    description='pfff harder DAG',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2022, 1, 1),
    catchup=False,
    tags=['example'],
) as dag:

    push1 = PythonOperator(
        task_id='print_airflow',
        python_callable=print_airflow,
    )
    pull2 = PythonOperator(
        task_id='pullinaa',
        python_callable=puller,
    )

    push1 >> pull2