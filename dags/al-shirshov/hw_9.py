from datetime import datetime, timedelta
from textwrap import dedent
from airflow import DAG

from airflow.operators.bash import BashOperator
from airflow.operators.python_operator import PythonOperator

import requests
import json


def push_data(ti):
    ti.xcom_push(
        key = "sample_xcom_key",
        value = "xcom test"
    )

def pull_data(ti):
    result = ti.xcom_pull(
        key = "sample_xcom_key",
        task_ids = 'push_value'
    )
    print(result)



with DAG(
    "hw_al-shirshov_9",
    default_args={
    'depends_on_past': False,
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),  # timedelta из пакета datetime
},
    start_date=datetime(2023,5,20),
    catchup=True   
) as dag:
    t1 = PythonOperator(
        task_id = 'push_value',
        python_callable = push_data
    )

    t2 = PythonOperator(
        task_id = 'pull_value',
        python_callable = pull_data
    )

    t1 >> t2
