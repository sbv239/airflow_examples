from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

import os

with DAG(
    'task_6',
    default_args={
        'depends_on_past': False,
        'email': ['airflow@example.com'],
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 1,
        'retry_delay': timedelta(minutes=5)
    },
    description='DAG for task 6',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2022, 3, 25),
    catchup=False,
    tags=['DP HW6']
) as dag:
        for i in range(10):
                os.environ['NUMBER'] = str(i)
                bash_task = BashOperator(
                        task_id=f'echo_task_{i}',
                        bash_command='echo $NUMBER'
                )


        def print_task_number(task: int, ts, run_id):
                print(ts)
                print(run_id)
                print(f'task number is {task}')


        for i in range(10, 30):
            python_task = PythonOperator(
                task_id=f'print_task_{i}',
                python_callable=print_task_number,
                op_kwargs={'task': i}
            )
