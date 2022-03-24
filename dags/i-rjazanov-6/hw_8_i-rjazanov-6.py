import os
from datetime import datetime, timedelta
from textwrap import dedent
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
import requests
import json

def get_x_com(ti):
    """
    Gets totalTestResultsIncrease field from Covid API for given state and returns value
    """
    testing_increase = "xcom test"
    # в ti уходит task_instance, его передает Airflow под таким названием
    # когда вызывает функцию в ходе PythonOperator
    ti.xcom_push(
        key='sample_xcom_key',
        value=testing_increase
    )


with DAG(
        # Название таск-дага
        'hw_8_i-rjazanov-6',

        default_args={
            'depends_on_past': False,
            'email': ['airflow@example.com'],
            'email_on_failure': False,
            'email_on_retry': False,
            'retries': 1,
            'retry_delay': timedelta(minutes=5),  # timedelta из пакета datetime

        },
        description='This is from DAG for Ryazanov to Task 8',
        schedule_interval=timedelta(days=1),
        start_date=datetime(2022, 3, 22),
        catchup=False,
        tags=['task_8']

) as dag:

    t1 = PythonOperator(
        task_id='get_xcom',
        python_callable=get_x_com,
        #op_kwargs={'task_number': i},
        )

    t1
