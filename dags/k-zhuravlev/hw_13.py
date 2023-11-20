from datetime import datetime, timedelta
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python import BranchPythonOperator
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.models import Variable
from airflow.operators.python import BranchPythonOperator


def branching():
    if Variable.get("is_startml") is "True":
        return "startml_desc"
    else:
        return "not_startml_desc"

def branch_1():
    print("StartML is a starter course for ambitious people")

def branch_2():
    print("Not a startML course, sorry")

with DAG(
    "task_13_k-zhuravlev",

default_args={
    'depends_on_past': False,
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),  # timedelta из пакета datetime
},

    start_date=datetime.now(),
    tags=["Cool_tag"]
) as dag:
    t1 = DummyOperator(
        task_id='before_branching'
    )

    t2 = BranchPythonOperator(
        task_id="determine_course",
        python_callable=branching
    )

    t3 = PythonOperator(
        task_id="startml_desc",
        python_callable=branch_1
    )

    t4 = PythonOperator(
        task_id="not_startml_desc",
        python_callable=branch_2
    )

    t5 = DummyOperator(
        task_id='after_branching'
    )

    t1 >> t2 >> [t3, t4] >> t5