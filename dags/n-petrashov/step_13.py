"""
Step 13
"""
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.models import Variable
from airflow.operators.dummy_operator import DummyOperator


def get_task_by_starml():
    if Variable.get("is_startml") == 'True':
        return "startml_desc"
    else:
        return "not_startml_desc"


def get_startml_message():
    print("StartML is a starter course for ambitious people")


def get_not_startml_message():
    print("Not a startML course, sorry")


with DAG(
        'petrashov_step_13',
        default_args={
            'depends_on_past': False,
            'email': ['airflow@example.com'],
            'email_on_failure': False,
            'email_on_retry': False,
            'retries': 1,
            'retry_delay': timedelta(minutes=1),
        },
        description='step_13 - solution',
        schedule_interval=timedelta(days=1),
        start_date=datetime(2023, 2, 11),
        catchup=False,
        tags=['step_13'],
) as dag:
    toggle = BranchPythonOperator(
        task_id='toggle_by_variable',
        provide_context=True,
        python_callable=get_task_by_starml
    )

    startml = PythonOperator(
        task_id='startml_desc',
        python_callable=get_startml_message
    )

    not_startml = PythonOperator(
        task_id='not_startml_desc',
        python_callable=get_not_startml_message
    )

    before_point = DummyOperator(
        task_id='before_start_point',
    )
    after_point = DummyOperator(
        task_id='after_start_point',
    )

    before_point >> toggle >> [startml, not_startml] >> after_point
