"""
Test documentation
"""
from datetime import datetime, timedelta

from airflow import DAG

from airflow.models import Variable
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python import (
    BranchPythonOperator,
    PythonOperator,
)


def branching() -> str:
    flag = Variable.get('is_startml')
    if flag == 'True':
        return 'startml_desc'
    return 'not_startml_desc'


def startml_desc():
    print("StartML is a starter course for ambitious people")


def not_startml_desc():
    print("Not a startML course, sorry")


with DAG(
        'task_12_r_baldaev',
        default_args={
            'depends_on_past': False,
            'email': ['airflow@example.com'],
            'email_on_failure': False,
            'email_on_retry': False,
            'retries': 1,
            'retry_delay': timedelta(minutes=5),  # timedelta из пакета datetime
        },
        description='Task 12 - branching',
        schedule_interval=timedelta(days=1),
        start_date=datetime(2022, 1, 1),
        catchup=False,
        tags=['r.baldaev-1'],
) as dag:
    task1 = DummyOperator(
        task_id='before_branching',
    )
    task2 = BranchPythonOperator(
        task_id='determine_course',
        python_callable=branching,
    )
    task3 = PythonOperator(
        task_id='startml_desc',
        python_callable=startml_desc,
    )
    task4 = PythonOperator(
        task_id='not_startml_desc',
        python_callable=not_startml_desc,
    )
    task5 = DummyOperator(
        task_id='after_branching',
    )

    task1 >> task2 >> [task3, task4] >> task5
