from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

import requests
import json


def pusher(ti):
    ti.xcom_push(
        key='sample_xcom_key',
        value="xcom test"
    )


def puller(ti):
    pull_value = ti.xcom_pull(
        key='sample_xcom_key',
        task_ids="pusher"
    )
    print(pull_value)
    return pull_value

with DAG(
    'blabla_7',

    default_args={
        'depends_on_past': False,
        'email': ['airflow@example.com'],
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 1,
        'retry_delay': timedelta(minutes=5),
    },
    description='very harder DAG',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2022, 1, 1),
    catchup=False,
    tags=['example'],
) as dag:

    push1 = PythonOperator(
        task_id='pusher',
        python_callable=pusher,
    )
    pull2 = PythonOperator(
        task_id='pullina',
        python_callable=puller,
    )

    push1 >> pull2