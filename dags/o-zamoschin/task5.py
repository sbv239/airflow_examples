"""
Start-ml Airflow Task 5
"""
import os
from datetime import datetime, timedelta
from textwrap import dedent

from airflow import DAG

from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

default_args={
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    'hw_5_o-zamoschin',
    default_args=default_args,
    schedule_interval=timedelta(days=1),
    start_date=datetime(2022, 1, 1),
    catchup=False,
) as dag:
    for i in range(10):
        t1 = BashOperator(
            task_id='echo_' + str(i),
            bash_command='echo $NUMBER',
            env = {"NUMBER" : str(i)},
        )
        
        t1.doc_md = dedent(
            """
            # Task Documentation
            This **task** prints *bash* command `echo` and the task number 
            """
        )

        t1
