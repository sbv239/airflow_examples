"""
KC Lesson 11 Airflow
Task 6
"""
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator


with DAG(
    'Task_6',
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
    tags=['i-nechushkin-20_Task_6'],
) as dag:

    for i in range(10):
        t = BashOperator(
            task_id=f'print_{i}',
            bash_command=f'echo $NUMBER',
            env={'NUMBER': i},
        )

    t