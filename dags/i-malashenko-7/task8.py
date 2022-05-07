"""
Test documentation
"""
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta
from textwrap import dedent

import requests
import json

# Операторы - это кирпичики DAG, они являются звеньями в графе
# Будем иногда называть операторы тасками (tasks)
with DAG(
    'hw_8_i-malashenko-7',
    # Параметры по умолчанию для тасок
    default_args={
        'depends_on_past': False,
        'email': ['airflow@example.com'],
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 1,
        'retry_delay': timedelta(minutes=5),  # timedelta из пакета datetime
    },
    schedule_interval=timedelta(days=1),
    start_date=datetime(2021, 3, 20),
    catchup=False,
    tags=['example'],
) as dag:

    def xcom_test_push(ti):
        ti.xcom_push(
            key='sample_xcom_key',
            value='xcom test'
        )

    def xcom_test_pull(ti):
        xcom_value = ti.xcom_pull(
            key='sample_xcom_key',
            task_ids='xcom_test_push'
        )
        print(xcom_value)

    t1 = PythonOperator(
        task_id='xcom_test_push',
        python_callable=xcom_test_push
    )   

    t2 = PythonOperator(
        task_id='xcom_test_pull',
        python_callable=xcom_test_pull
    )

    t1 >> t2