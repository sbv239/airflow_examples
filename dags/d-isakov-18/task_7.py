"""
Task 6
"""
from datetime import datetime, timedelta

from airflow import DAG

from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

with DAG(
    'hw_6_d-isakov-18',
    default_args={
        'depends_on_past': False,
        'email': ['airflow@example.com'],
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 1,
        'retry_delay': timedelta(minutes=5),
    },
    description=__doc__,
    schedule_interval=timedelta(days=1),
    start_date=datetime(2023, 4, 1),
    catchup=False,
) as dag:

    def print_task_number(ts, run_id, **kwargs):
        print(f'task number is: {str(kwargs["task_number"])}')
        print(f'ts: {str(ts)}')
        print(f'run_id: {str(run_id)}')

    for i in range(10):
        t1 = PythonOperator(
            task_id=f'python_task_{i}',
            python_callable=print_task_number,
            op_kwargs={'task_number': i}
        )

    t1