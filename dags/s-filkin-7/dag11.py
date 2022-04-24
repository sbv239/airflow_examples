from datetime import datetime, timedelta
from textwrap import dedent

from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.dummy import DummyOperator
from airflow.models import Variable


def what_next():
    is_startml = Variable.get("is_startml")
    if is_startml == "True":
        return "startml_desc"
    else:
        return "not_startml_desc"

def python_startml_desc():
    print("StartML is a starter course for ambitious people")

def python_not_startml_desc():
    print("Not a startML course, sorry")


with DAG(
    's-filkin-7-dag11',
    default_args={
        'depends_on_past': False,
        'email': ['airflow@example.com'],
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 1,
        'retry_delay': timedelta(minutes=5),
    },
    schedule_interval=timedelta(days=1),
    start_date=datetime(2022, 4, 23),
) as dag:

    start = DummyOperator(task_id='start')
    finish = DummyOperator(task_id='finish')

    branching = BranchPythonOperator(
        task_id='branching',
        python_callable=what_next,
    )

    t1 = PythonOperator(
        task_id='startml_desc', 
        python_callable=python_startml_desc,
    )

    t2 = PythonOperator(
        task_id='not_startml_desc', 
        python_callable=python_not_startml_desc,
    )

    start >> branching >> [t1, t2] >> finish