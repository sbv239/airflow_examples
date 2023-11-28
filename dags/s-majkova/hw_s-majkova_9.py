from datetime import datetime, timedelta
from textwrap import dedent
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
import requests
import json
def xcom_push(ti):
    ti.xcom_push(
        key='sample_xcom_key',
        value= 'xcom test'
    )
def xcom_pull(ti):
    ti.xcom_pull(
        key='sample_xcom_key',
        task_ids= 'xcom_push'
    )


with DAG(
    'hw_s-majkova_9',
    default_args = {
    'depens_on_past': False,
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes = 5),
    },
    description = 'xcom_DAG',
    max_active_runs=2,
    schedule_interval=timedelta(minutes=30),
    start_date = datetime(2023, 11, 26),
    catchup = False,
    tags = ['hw_9'],
) as dag:
    t1 = PythonOperator(
        task_id = 'xcom_push',
        python_callable =  xcom_push
    )
    t2 = PythonOperator(
        task_id = 'xcom_pull',
        python_callable = xcom_pull
    )
t1>>t2