from datetime import datetime, timedelta
from textwrap import dedent
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.operators.python import BranchPythonOperator

from airflow.models import Variable
from airflow.hooks.base import BaseHook
from airflow.providers.postgres.operators.postgres import PostgresHook
from airflow.operators.dummy_operator import DummyOperator

def branch_13():
    if Variable.get("is_startml") == "True":
        return "startml_desc"
    else:
        return "not_startml_desc"


def print_startml():
    print("StartML is a starter course for ambitious people")


def print_not_startml():
    print("Not a startML course, sorry")

with DAG(
    'hw_13_i-loskutov',
    default_args={
    'depends_on_past': False,
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),  
},

    description='task13',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2023, 5, 27),
    catchup=False

) as dag:
    t1 = DummyOperator(
        task_id = 'start_13'
    )

    t2 = BranchPythonOperator(
        task_id = 'Branch_13',
        python_callable = branch_13
    )
    t3 = PythonOperator(
        task_id = 'startml_desc',
        python_callable = print_startml
    )

    t4 = PythonOperator(
        task_id = 'not_startml_desc',
        python_callable = print_not_startml
    )

    t5 = DummyOperator(
        task_id = 'end_13'
    )

    t1 >> t2 >> [t3, t4] >> t5





