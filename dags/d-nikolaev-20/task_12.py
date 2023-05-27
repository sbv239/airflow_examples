from airflow import DAG

from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta
import json
from airflow.operators.dummy import DummyOperator
from airflow.operators.python import BranchPythonOperator

str_text = 'xcom test'


def branch():
    from airflow.models import Variable

    is_startml = Variable.get("is_startml")
    if is_startml == 'True':
        return 'startml_desc'
    return 'not_startml_desc'


def task_12_func_1():
    print("StartML is a starter course for ambitious people")


def task_12_func_2():
    print("Not a startML course, sorry")


default_args = {
    'depends_on_past': False,
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),  # timedelta из пакета datetime
}

with DAG(
        'hw_12_d-nikolaev',
        start_date=datetime(2021, 1, 1),
        max_active_runs=2,
        schedule_interval=timedelta(minutes=30),
        default_args=default_args,
        catchup=False
) as dag:
    task_1 = PythonOperator(
        task_id='startml_desc',
        python_callable=task_12_func_1,
    )
    task_2 = PythonOperator(
        task_id='not_startml_desc',
        python_callable=task_12_func_2
    )
    branching = BranchPythonOperator(
        task_id='branching',
        python_callable=branch,
    )
    run_first = DummyOperator(
        task_id='run_first',
    )
    run_end = DummyOperator(
        task_id='run_end',
    )
    run_first>>branching>>[task_1, task_2]>>run_end
