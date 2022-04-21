from datetime import datetime, timedelta
from textwrap import dedent

from airflow import DAG

from airflow.operators.bash import BashOperator
from airflow.operators.python_operator import PythonOperator

import requests
import json

with DAG(
    'dag_8_d-luzina-7',
    default_args={
        'depends_on_past': False,
        'email': ['airflow@example.com'],
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 1,
        'retry_delay': timedelta(minutes=5),  
    },
    description='dag 8 lesson 11',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2022, 1, 1),
    catchup=False,
    tags=['example'],
    
) as dag:
    def func_push(ti):
        ti.xcom_push(
            key='sample_xcom_key',
            value='xcom test'
        )
    
    t1 = PythonOperator(
        task_id = 'push',
        python_callable=func_push
    )
    
    def func_pull(ti):
        output = ti.xcom_pull(
            key='sample_xcom_key',
            task_ids='push'
        )
        print(output)
    
    t2 = PythonOperator(
        task_id = 'pull',
        python_callable=func_pull
    )
    
    t1 >> t2