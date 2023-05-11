from datetime import datetime, timedelta
from textwrap import dedent

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

with DAG(
    'hw_p-ratibor-20_2',
    start_date=datetime(2023, 5, 11),
    default_args={
        'depends_on_past': False,
        'email': ['airflow@example.com'],
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 1,
        'retry_delay': timedelta(minutes=5),
    },
) as dag:
    for i in range(1, 11):
        bash_task = BashOperator(
            task_id=f'task{i}',
            bash_command=f"echo {i}"
        )

    def print_task_number(n):
        print("task number is: {n}")

    for i in range(11, 31):
        python_task = PythonOperator(
            task_id=f'task{i}',
            python_callable=print_task_number,
            op_kwargs={'n': {i}}
        )