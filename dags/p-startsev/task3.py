from datetime import datetime, timedelta
from textwrap import dedent

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

with DAG(
    'hw_3_p-startsev',
    start_date=datetime(2023, 11, 25),
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
    def print_task_number(task_number):
        print("task number is: {task_number}")
    
    for i in range(11, 31):
        python_task = PythonOperator(
            task_id=f'task{i}',
            python_callable=print_task_number,
            op_kwargs={'n': {i}}
        )