"""
DAG for task 2
"""
from datetime import timedelta, datetime

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

with DAG(
        'hw_t-togyzbaev_6',
        default_args={
            'depends_on_past': False,
            'email': ['airflow@example.com'],
            'email_on_failure': False,
            'email_on_retry': False,
            'retries': 1,
            'retry_delay': timedelta(minutes=5),  # timedelta из пакета datetime
        },
        description='First task',
        schedule_interval=timedelta(days=1),
        start_date=datetime(2022, 1, 1),
        catchup=False,
        tags=['t-togyzbaev']
) as dag:
    for idx in range(10):
        BashOperator(
            task_id=f"bash_echo_{idx}",
            bash_command=f"echo $NUMBER",
            env={
                "NUMBER": str(idx)
            }
        )


    def print_idx(task_number):
        print(f"task number is: {task_number}")


    for idx in range(10, 30):
        PythonOperator(
            task_id=f"python_echo_{idx}",
            python_callable=print_idx,
            op_kwargs={
                "task_number": idx
            }
        )
