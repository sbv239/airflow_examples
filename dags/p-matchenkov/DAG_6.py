import datetime

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from textwrap import dedent

from datetime import timedelta


def print_task_number(task_number):
    print(f'task number is: {task_number}')


with DAG(
    'hw_p-matchenkov_6',
    default_args={
        'depends_on_past': False,
        'email': ['airflow@example.com'],
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 1,
        'retry_delay': timedelta(minutes=5),
    },
    description='task 6 dag',
    start_date=datetime.datetime(2023, 10, 16),
    catchup=False,
    tags=['matchenkov']
) as dag:

    for i in range(10):
        if i < 10:
            bash_task = BashOperator(
                task_id=f'bash_step_{i}',
                env={'NUMBER': i},
                bash_command=f'echo $NUMBER'
            )

    bash_task