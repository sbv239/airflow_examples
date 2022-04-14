from datetime import datetime, timedelta
from textwrap import dedent

from airflow import DAG

from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

with DAG(
    'intro_6th',
    default_args={
        'depends_on_past': False,
        'email': ['airflow@example.com'],
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 1,
        'retry_delay': timedelta(minutes=5), 
    },
    description='intro_6th',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2022, 3, 10),
    catchup=False,
    tags=['a-jablokova'],
) as dag:

    def print_num(task_number, ts, run_id):
        print(f"task number is: {task_number}")
        print(f'ts: {ts}')
        print(f'run_id: {run_id}')

    for i in range(10):
        t2 = PythonOperator(
            task_id = 'python_' + str(i),
            python_callable = print_num,
            op_kwargs = {'task_number': i},
        )

    t2