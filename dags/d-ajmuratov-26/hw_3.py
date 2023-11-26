"""
Hello, Airflow Documentation!!!
"""
from textwrap import dedent

from datetime import timedelta
from datetime import datetime

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

default_args={
    'depends_on_past': False,
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),  # timedelta из пакета datetime
}
with DAG(
    'hw_d-ajmuratov-26_3',
    default_args=default_args,
    start_date=datetime.now(),
    catchup=False,
    tags=['HM3']
) as dag:
    def print_curr_task_number(task_number):
        print(f"task number is: {task_number}")
    for i in range(30):
        if i < 10:
            t = BashOperator(
                task_id=f'echo_curr_task_number_{i}',
                bash_command=f'echo {i}'
            )
        else:
            t = PythonOperator(
                task_id=f'print_curr_task_number_{i}',
                python_callable=print_curr_task_number,
                op_args=i
            )
        t