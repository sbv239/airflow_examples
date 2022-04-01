"""
 Airflow trials

"""
from datetime import datetime, timedelta
from textwrap import dedent

from airflow import DAG

from airflow.operators.bash import BashOperator
from airflow.operators.python_operator import PythonOperator

def try_branching():
    if is_startml == True:
        return "startml_desc"
    else:
        return "not_startml_desc"

def startml_desc():
    return "StartML is a starter course for ambitious people"

def not_startml_desc():
    return "Not a startML course, sorry" 

 

with DAG(
's-filonov-6_hw11',
default_args={
    'depends_on_past': False,
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),  
},

description='A simple tutorial DAG',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2022, 3, 28),
    catchup=False,
    tags=['learning'],
) as dag:

    t1 = DummyOperator(
        task_id='run_this_first',
    )

    branching = BranchPythonOperator(
        task_id='branching',
        python_callable=try_branching
    )


    t2 = DummyOperator(
        task_id='run_this_last',
    )

    t1 >> branching >> t2