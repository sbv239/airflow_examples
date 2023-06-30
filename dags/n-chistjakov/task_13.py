from datetime import datetime, timedelta

from airflow import DAG

from airflow.models import Variable 
from airflow.operators.python_operator import PythonOperator
from airflow.operators.python import BranchPythonOperator
from airflow.operators.dummy import DummyOperator

ID = "hw_13_n-chistjakov_"

def branching():
    return 'startml_desc' if Variable.get("is_startml") == "True" else 'not_startml_desc'

def startml():
    print("StartML is a starter course for ambitious people")

def not_startml():
    print("Not a startML course, sorry")

with DAG(
    'hw_13_n-chistjakov',
    default_args={
    'depends_on_past': False,
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    },
    description="Second task",
    start_date=datetime(2023, 6, 30),
    schedule_interval=timedelta(days=1),
    catchup=False,
    tags=["task_13"],
) as dag:

    before = DummyOperator(
        task_id="before_branching"
    )

    branch = BranchPythonOperator(
        task_id="branching",
        python_callable=branching
    )

    pyth_is_startml = PythonOperator(
        task_id="startml_desc",
        python_callable=startml
    )

    pyth_isnt_startml = PythonOperator(
        task_id="not_startml_desc",
        python_callable=not_startml
    )

    after = DummyOperator(
        task_id="after_branching"
    )

    before >> branch >> [pyth_is_startml, pyth_isnt_startml] >> after