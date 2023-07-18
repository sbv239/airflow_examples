"""
--DAG docs will be there--
"""
from datetime import datetime, timedelta
from textwrap import dedent
from airflow import DAG

# from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.dummy_operator import DummyOperator


def determine_course(*args, **kwargs):
    from airflow.models import Variable
    if Variable.get("is_startml")=='True':
        return "startml_desc"
    else:
        return "not_startml_desc"


def startml_desc(*args, **kwargs):
    print("StartML is a starter course for ambitious people")


def not_startml_desc(*args, **kwargs):
    print("Not a startML course, sorry")


default_args={
    'depends_on_past': False,
    'owner': 'and-f',
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(dag_id='hw_13_and-f',
         default_args=default_args,
         description='--DAG description here--',
         schedule_interval=timedelta(days=1),
         start_date=datetime(2023, 7, 17),
         catchup=False,
         tags=['--DAG tag here--']) as dag:

    dag.doc_md = __doc__
    t1 = DummyOperator(task_id='before_branching')
    t2 = BranchPythonOperator(task_id='determine_course', python_callable=determine_course)
    t3 = PythonOperator(task_id='startml_desc', python_callable=startml_desc)
    t4 = PythonOperator(task_id='not_startml_desc', python_callable=not_startml_desc)
    t5 = DummyOperator(task_id='after_branching')

    t1 >> t2 >> [t3, t4] >> t5