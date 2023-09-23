"""
Test documentation
"""
from datetime import datetime, timedelta
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow import DAG
from airflow.models import Variable
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import BranchPythonOperator, PythonOperator

def get_choose():
    if Variable.get('is_startml'):
        return 'startml_desc'
    else:
        return 'not_startml_desc'

def print_right_choose():
    print("StartML is a starter course for ambitious people")

def print_wrong_choose():
    print("Not a startML course, sorry")


with DAG(
        default_args={
            'depends_on_past': False,
            'email': ['airflow@example.com'],
            'email_on_failure': False,
            'email_on_retry': False,
            'retries': 1,
            'retry_delay': timedelta(minutes=5)},

        start_date=datetime(2023, 9, 22),
        dag_id="hw_13_a-ratushnyj",
        schedule_interval=timedelta(days=1),
        tags=['hw-13'],

) as dag:

    first = DummyOperator(
        task_id='first'
    )

    choose_way = BranchPythonOperator(
        task_id='choose_way',
        python_callabel=get_choose
    )

    right_way = PythonOperator(
        task_id="startml_desc",
        python_callabel=print_right_choose
    )

    wrong_way = PythonOperator(
        task_id="not_startml_desc",
        python_callabel=print_wrong_choose
    )

    last = DummyOperator(
        task_id='last'
    )

    first >> choose_way >> [right_way,wrong_way] >> last