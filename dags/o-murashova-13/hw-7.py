from datetime import datetime, timedelta
from textwrap import dedent
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator


with DAG(
    'hw_4_o-murashova-13',
    default_args={
        'depends_on_past': False,
        'email': ['airflow@example.com'],
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 1,
        'retry_delay': timedelta(minutes=5),},
    description='A simple tutorial DAG',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2022, 11, 11),
    tags=['example'],
) as dag:
    def print_(ts, run_id):
        print(ts)
        print(run_id)


    for i in range(20):
        t2 = PythonOperator(
            task_id=f'pitosha_{i}',
            python_callable=print_,
        )
