from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator

import os

with DAG(
    'task_5',
    default_args={
        'depends_on_past': False,
        'email': ['airflow@example.com'],
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 1,
        'retry_delay': timedelta(minutes=5)
    },
    description='DAG for task 5',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2022, 3, 25),
    catchup=False,
    tags=['DP HW5']
) as dag:
        for i in range(10):
                os.environ['NUMBER'] = str(i)
                bash_task = BashOperator(
                        task_id=f'echo_task_{i}',
                        bash_command='echo $NUMBER'
                )


        def print_task_number(task: int):
                print(f'task number is {task}')
