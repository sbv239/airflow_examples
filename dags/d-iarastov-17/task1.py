from airflow import DAG
from airflow.operators.bash import BashOperator 
from airflow.operators.python import PythonOperator

from datetime import timedelta
from datetime import datetime

from textwrap import dedent

def print_context(ds, **kwargs):
    print(kwargs)
    print(ds)
    return 'Whatever you return gets printed in the logs'

with DAG(
    'my_first_dag_123',
    default_args={
        'depend_on_past': False,
        'email':['airflow@example.com'],
        'email_on_failure':False,
        'email_on_retry':False,
        'retries':1,
        'retry_delay': timedelta(minutes=5)
    },
    # Описание DAG (не тасок, а самого DAG)
    description='A simple tutorial DAG',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2022, 1, 1),
    catchup=False,
    tags = ['task2_lms']
) as dag:
    t1 = BashOperator(
        task_id='bash_print_directory',
        bash_command='pwd'
    ),
    t2 = PythonOperator(
    task_id = 'python_date',
    python_callable=print_context
    )
    t1 >> t2