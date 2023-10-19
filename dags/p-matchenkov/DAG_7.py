import datetime

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from textwrap import dedent

from datetime import timedelta


def print_task_number(ts, run_id, **kwargs):
    print(f'ts = {ts}')
    print(f'run_id = {run_id}')
    print(f'task number is: {kwargs["task_number"]}')


with DAG(
    'hw_p-matchenkov_7',
    default_args={
        'depends_on_past': False,
        'email': ['airflow@example.com'],
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 1,
        'retry_delay': timedelta(minutes=5),
    },
    description='task 7 dag',
    start_date=datetime.datetime(2023, 10, 16),
    catchup=False,
    tags=['matchenkov']
) as dag:

    for i in range(30):
        if i < 10:
            bash_task = BashOperator(
                task_id=f'bash_task_{i}',
                bash_command=f'echo {i}'
            )

        else:
            python_task = PythonOperator(
                task_id=f'current_task_{i}',
                python_callable=print_task_number,
                op_kwargs={'number': i} # передает данные в питоновскую функцию,
            )

    bash_task >> python_task