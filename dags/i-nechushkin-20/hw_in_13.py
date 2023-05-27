"""
Lesson KC Airflow
Task 13
"""
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.python import BranchPythonOperator
from airflow.operators.dummy import DummyOperator


def _choose_desk():
    from airflow.models import Variable
    is_startml_var = Variable.get("is_startml")
    if is_startml_var == 'True':
        return 'startml_desc'
    else:
        return 'not_startml_desc'

def print_startml_desc():
    print("StartML is a starter course for ambitious people")

def print_not_startml_desc():
    print("Not a startML course, sorry")


with DAG(
    'Task_13',
    # DAG dafault parameters
    default_args={
        'depends_on_past': False,
        'email': ['airflow@example.com'],
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 1,
        'retry_delay': timedelta(minutes=5),
    },
    start_date=datetime.now(),
    tags=['i-nechushkin-20_Task_13'],
) as dag:

    dummy_1 = DummyOperator(
        task_id='dummy_1'
    )

    choose_desk = BranchPythonOperator(
        task_id='choose_desk',
        python_callable = _choose_desk,
        do_xcom_push=False
    )

    t1 = PythonOperator(
        task_id='startml_desc',
        python_callable=print_startml_desc,
    )

    t2 = PythonOperator(
        task_id='not_startml_desc',
        python_callable=print_not_startml_desc,
    )

    dummy_2 = DummyOperator(
        task_id='dummy_2',
        trigger_rule='none_failed_or_skipped'
    )

    dummy_1 >> choose_desk >> [t1, t2] >> dummy_2
