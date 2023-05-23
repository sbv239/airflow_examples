"""
KC Lesson 11 Airflow
Task 3
"""
from datetime import datetime, timedelta
from textwrap import dedent

from airflow import DAG

from airflow.operators.bash import BashOperator
from airflow.operators.python_operator import PythonOperator


with DAG(
    'Task_2',
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
    tags=['Task_2'],
) as dag:

    for i in range(10):
        t1 = BashOperator(
            task_id=f'print_{i+1}',
            bash_command=f'echo {i+1}',
        )

    def print_task_num(task_number):
        print(f"task number is: {task_number}")
        return None

    for i in range(20):
        t2 = PythonOperator(
            task_id=f'print_task_num{i}',
            python_callable=print_task_num,
            op_kwargs={'task_number': i},
        )

    t1.doc_md = dedent(
    """
    ## Task **«t1»** documentation
    This task is repeated *10* times using a `for` loop.
    """
    )

    t2.doc_md = dedent(
    """
    ## Task **«t2»** documentation
    This task is repeated *20* times using a `for` loop and *passes* parameters to the function.
    """
    )

    t1 >> t2
