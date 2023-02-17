from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta

with DAG(
    'examples',
default_args={
    'depends_on_past': False,
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
},
description='A simple tutorial DAG',
schedule_interval=timedelta(days=1),
start_date=datetime(2022, 1, 1),
catchup=False,
tags=['example'],
) as dag:

    t1 = BashOperator(
        task_id='direction',
        bash_command='pwd',
    )

    def print_context(ds):

        print(ds)
        return 'Hello world'

    t2=PythonOperator(
        task_id='print_ds',
        python_callable=print_context,
    )
    t1>>t2