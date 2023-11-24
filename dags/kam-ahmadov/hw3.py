"""
Homework 2 First Dag
"""
from datetime import datetime, timedelta
from textwrap import dedent
from airflow import DAG

from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

with DAG(
    'hw_kamilahmadov_3',
    default_args={
        'depends_on_past': False,
        'email': ['airflow@example.com'],
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 1,
        'retry_delay': timedelta(minutes=5),
    },
    description='first DAG',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2023, 11, 20),
    catchup=False,
    tags=["hw_3"]
) as dag:
    
    def print_task(task, **kwargs):
        print(kwargs)
        print(task)

    for i in range(10):
        t1 = BashOperator(
            task_id=f"random_bash{i}",
            bash_command=f"echo {i}"
        )

    for i in range(20):
        t2 = PythonOperator(
            task_id=f"task_{i}",
            python_callable=print_task,
            op_kwargs={"task number is": f"task_{i}"}
        )