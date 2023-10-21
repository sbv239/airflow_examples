import datetime

import airflow.hooks.base
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.models import Variable
from textwrap import dedent
from airflow.operators.empty import EmptyOperator

from datetime import timedelta


def get_task_id():
    task_ids = ['startml_desc', 'not_startml_desc']
    var = Variable.get('is_startml')
    if var == 'True':
        return task_ids[0]
    elif var == 'False':
        return task_ids[1]


def startml():
    print("StartML is a starter course for ambitious people")


def not_startml():
    print("Not a startML course, sorry")


with DAG(
    'hw_p-matchenkov_13',
    default_args={
        'depends_on_past': False,
        'email': ['airflow@example.com'],
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 1,
        'retry_delay': timedelta(minutes=5),
    },
    description='branching',
    tags=['matchenkov'],
    catchup=False,
    start_date=datetime.datetime(2023, 10, 20)
) as dag:

    start_point = EmptyOperator(
        task_id='start'
    )


    branching = BranchPythonOperator(
        task_id='branching',
        python_callable=get_task_id
    )


    startml_task = PythonOperator(
        task_id='startml_desc',
        python_callable=startml
    )


    not_startml_task = PythonOperator(
        task_id='not_startml_desc',
        python_callable=not_startml
    )


    finish_point = EmptyOperator(
        task_id='finish'
    )

    startml_task >>  branching >> [startml_task, not_startml_task] >> finish_point



