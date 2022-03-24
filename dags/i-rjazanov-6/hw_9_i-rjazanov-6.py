import os
from datetime import datetime, timedelta
from textwrap import dedent
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
import requests
import json

def return_string():
    return 'Airflow tracks everything'

def get_xcom(ti):
    value_read = ti.xcom_pull(
        key='return_value',
        task_ids='return_xcom')
    print(value_read)

with DAG(
        # Название таск-дага
        'hw_9_i-rjazanov-6',

        default_args={
            'depends_on_past': False,
            'email': ['airflow@example.com'],
            'email_on_failure': False,
            'email_on_retry': False,
            'retries': 1,
            'retry_delay': timedelta(minutes=5),  # timedelta из пакета datetime

        },
        description='This is from DAG for Ryazanov to Task 9',
        schedule_interval=timedelta(days=1),
        start_date=datetime(2022, 3, 22),
        catchup=False,
        tags=['task_9']

) as dag:

    t1 = PythonOperator(
        task_id='return_xcom',
        python_callable=return_string,
        #op_kwargs={'task_number': i},
    )

    t2 = PythonOperator(
        task_id='get_xcom',
        python_callable=get_xcom,
        #op_kwargs={'task_number': i},
    )


    t1 >> t2