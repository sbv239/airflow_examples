import os
from datetime import datetime, timedelta
from textwrap import dedent
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
import requests
import json

conn_id="startml_feed"

def get_variable():
    from airflow.models import Variable
    print(Variable.get("is_startml"))


with DAG(
        # Название таск-дага
        'hw_11_i-rjazanov-6',

        default_args={
            'depends_on_past': False,
            'email': ['airflow@example.com'],
            'email_on_failure': False,
            'email_on_retry': False,
            'retries': 1,
            'retry_delay': timedelta(minutes=5),  # timedelta из пакета datetime

        },
        description='This is from DAG for Ryazanov to Task 11',
        schedule_interval=timedelta(days=1),
        start_date=datetime(2022, 3, 22),
        catchup=False,
        tags=['task_11']

) as dag:

    t1 = PythonOperator(
        task_id='get_variable',
        python_callable=get_variable,
        #op_kwargs={'task_number': i},
    )

    t1