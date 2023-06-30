from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import requests
import json
def push(ti):
    ti.xcom_push(
        key='sample_xcom_key',
        value="xcom test"
    )
def pull(ti):
    testing_increases = ti.xcom_pull(
        key='sample_xcom_key',
        task_ids='pull_data'
    )
    print('Testing increases for :', testing_increases)

# Default settings applied to all tasks
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}

with DAG(
    'hw_fe-denisenko-21_9_step',
    start_date=datetime(2023, 6, 20),
    max_active_runs=2,
    schedule_interval=timedelta(minutes=30),
    default_args=default_args,
    catchup=False
) as dag:
    first = PythonOperator(
        task_id = 'pull_data',
        python_callable=push
    )
    second = PythonOperator(
        task_id = 'analyze_data',
        python_callable=pull
    )
    first >> second
