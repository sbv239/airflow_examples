from datetime import datetime, timedelta
from textwrap import dedent
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

def print_ds(ds):
    print(ds)

with DAG(
    'g-karateev_d1',
    default_args={
        'depends_on_past': False,
        'email': ['airflow@example.com'],
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 1,
        'retry_delay': timedelta(minutes=5),  # timedelta из пакета datetime
    },
    desctiption = 'A simple DAG for task 1',
    schedule_interval = timedelta(days=1),
    start_date = datetime(2022, 5, 5),
    catchup = False,
    tags = ['example']
) as dag:
    t1 = BashOperator(
        task_id = 'find_pwd',
        bash_command = 'pwd'
    )
    t2 = PythonOperator(
        task_id = 'print ds',
        python_callable = print_ds
    )

    t1 >> t2
