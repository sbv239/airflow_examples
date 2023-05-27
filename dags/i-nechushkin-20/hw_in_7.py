"""
KC Lesson 11 Airflow
Task 7
"""
from datetime import datetime, timedelta
from textwrap import dedent

from airflow import DAG

from airflow.operators.bash import BashOperator
from airflow.operators.python_operator import PythonOperator


with DAG(
    'Task_7',
    # DAG dafault parameters
    default_args={
        'depends_on_past': False,
        'email': ['airflow@example.com'],
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 1,
        'retry_delay': timedelta(minutes=5),
    },
    start_date=datetime.now(),
    tags=['i-nechushkin-20_Task_7'],
) as dag:

    for i in range(10):
        t1 = BashOperator(
            task_id=f'print_{i+1}',
            bash_command=f'echo {i+1}',
        )

    def print_task_num(ts, run_id, **kwargs):
        print(f"task number is: {task_number}")
        print(ts)
        print(run_id)
        return None

    for i in range(20):
        t2 = PythonOperator(
            task_id=f'print_task_num{i}',
            python_callable=print_task_num,
            op_kwargs={'task_number': i},
        )

    t1 >> t2
