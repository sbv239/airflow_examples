import os
from datetime import datetime, timedelta
from textwrap import dedent
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python_operator import PythonOperator

with DAG(
    'g-sheverdin-7_task05',
    default_args={
        'depends_on_past': False,
        'email': ['airflow@example.com'],
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 1,
        'retry_delay': timedelta(minutes=5),
    },
    description='g-sheverdin-7_DAG_task05',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2022, 4, 11),
    catchup=False,
    tags=['g-sheverdin-7-task05'],
) as dag:

    for i in range(1 , 11):
        os.environ['NUMBER'] = str(i)
        t1 = BashOperator(
            task_id='echo_' + str(i),
            bash_command="echo '{}'".format(os.environ['NUMBER']),
        )

    t1
