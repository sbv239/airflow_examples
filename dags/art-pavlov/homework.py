from datetime import datetime, timedelta
from textwrap import dedent

from airflow import  DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

def print_context(ds, **kwargs):
    print(kwargs)
    print(ds)
    return "Something"


with DAG(
    "first_task",

default_args={
    'depends_on_past': False,
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),  # timedelta из пакета datetime
},
    description= "first try of starting DAG's",
    schedule_interval= timedelta(days=1),
    start_date= datetime(2023, 4, 17),
    catchup= False,
    tags=['idk']
) as dag:

    t1 = BashOperator(
        task_id='current_direction',
        bash_command="pwd"
    )

    t2 = PythonOperator(
        task_id='print_ds',
        python_callable=print_context
    )

    t1 >> t2