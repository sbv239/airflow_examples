"""
Variables and Branching
"""
from airflow import DAG
from airflow.models import Variable

from datetime import datetime, timedelta

from airflow.operators.python import PythonOperator
from airflow.operators.dummy import DummyOperator
from airflow.operators.python import BranchPythonOperator

default_args = {
    'depends_on_past': False,
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),  # timedelta из пакета datetime
}


def choose():
    if Variable.get('is_startml') == 'True':
        return 'startml_desc'
    return 'not_startml_desc'


def startml_desc():
    print('StartML is a starter course for ambitious people')


def not_startml_desc():
    print('Not a startML course, sorry')


with DAG(
        'f-dubjago-7_dag12',
        default_args=default_args,
        schedule_interval=timedelta(days=1),
        start_date=datetime(2022, 4, 10),
        tags=['df'],
) as dag:
    before_branching = DummyOperator(
        task_id='before_branching',
    )

    branching = BranchPythonOperator(
        task_id='determine_course',
        python_callable=choose,
    )

    startml_desc = PythonOperator(
        task_id='startml_desc',
        python_callable=startml_desc,
    )

    not_startml_desc = PythonOperator(
        task_id='not_startml_desc',
        python_callable=not_startml_desc,
    )

    after_branching = DummyOperator(
        task_id='after_branching',
    )

    before_branching >> branching >> [startml_desc, not_startml_desc] >> after_branching
