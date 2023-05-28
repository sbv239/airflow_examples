from datetime import datetime, timedelta
from textwrap import dedent
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

from airflow.models import Variable
from airflow.hooks.base import BaseHook
from airflow.providers.postgres.operators.postgres import PostgresHook

def print_var():
    print(Variable.get("is_startml"))



with DAG(
    'hw_12_i-loskutov',
    default_args={
    'depends_on_past': False,
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),  
},

    description='task12',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2023, 5, 27),
    catchup=False

) as dag:
    t1 = PythonOperator(
        task_id = 'print_var',
        python_callable=print_var
    )

    t1





