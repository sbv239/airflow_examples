from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta

import requests
import json

def xcom_push(ti):
    # в ti уходит task_instance, его передает Airflow под таким названием
    # когда вызывает функцию в ходе PythonOperator
    ti.xcom_push(
        key='sample_xcom_key',
        value='xcom test'
    )

def xcom_pull(ti):
    test = ti.xcom_pull(
        key='sample_xcom_key',
        task_ids='get_data'
    )
    print(test)

# Default settings applied to all tasks
with DAG (
    'hw_d-orlova-21_9',
    default_args={
        'depends_on_past': False,
        'email': ['airflow@example.com'],
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 1,
        'retry_delay': timedelta(minutes=5),  # timedelta из пакета datetime
    },
    description = 'dag for lesson 11.9',
    schedule_interval = timedelta(days=1),
    start_date=datetime(2023, 6, 28),
    catchup = False
) as dag:

    get_data = PythonOperator(
        task_id = 'get_data',
        python_callable=xcom_push
    )
    analyse_data = PythonOperator(
        task_id = 'analyze_data',
        python_callable=xcom_pull
    )

    get_data >> analyse_data