from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.dummy import DummyOperator
from datetime import datetime, timedelta
from airflow.models import Variable


def branching_func():
    if Variable.get('is_startml') == 'True':
        task_id = "startml_desc"
    else:
        task_id = "not_startml_desc"
    return task_id


def startml_desc_func():
    return "StartML is a starter course for ambitious people"


def not_startml_desc_func():
    return "Not a startML course, sorry"


default_args = {
    'depends_on_past': False,
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
        'hw_13_d-korjakov',
        description='variable DAG',
        default_args=default_args,
        start_date=datetime(2023, 9, 24),
        schedule_interval=timedelta(days=1),
) as dag:
    t1 = PythonOperator(
        task_id='startml_desc',
        python_callable=startml_desc_func,
    )

    t2 = PythonOperator(
        task_id='not_startml_desc',
        python_callable=not_startml_desc_func,
    )
    start_operator = DummyOperator(task_id='start')
    end_operator = DummyOperator(task_id='end')

    branching = BranchPythonOperator(
        task_id='branching_task',
        python_callable=branching_func,
    )

start_operator >> branching >> end_operator
