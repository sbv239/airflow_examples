"""
Test documentation
"""
from datetime import datetime, timedelta
from textwrap import dedent

from airflow import DAG

from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator


def print_context(ds, **kwargs):
    # print(kwargs)
    print(ds)
    return 'Whatever you return gets printed in the logs'


default_args={
        'depends_on_past': False,
        'email': ['airflow@example.com'],
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 1,
        'retry_delay': timedelta(minutes=5),  # timedelta из пакета datetime
    }
    
with DAG(
    'Task_2',

    default_args=default_args,
    
    description='Task 2',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2023, 1, 1),
    catchup=False,
    tags=['Task 2'],
) as dag:

    t1 = BashOperator(
        task_id = 'BashOperator',
        bash_command = 'pwd',
    )

    t2 = PythonOperator(
    task_id='PythonOperator',
    python_callable=print_context,
    )

t1 >> t2