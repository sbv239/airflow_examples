from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator
from datetime import datetime, timedelta

from airflow.models import Variable


def dummy():
    pass

def check_branch():
    is_startml = Variable.get('is_startml')  # необходимо передать имя, заданное при создании Variable
    if is_startml == 'True':
        return 'startml_desc'
    
    return 'not_startml_desc'

def print_if_startml():
    print('StartML is a starter course for ambitious people')

def print_if_not_startml():
    print('Not a startML course, sorry')

# Default settings applied to all tasks
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}

with DAG(
    'hw_13_a-korenevich-1',
    start_date=datetime(2021, 1, 1),
    max_active_runs=2,
    schedule_interval=timedelta(minutes=30),
    default_args=default_args,
    catchup=False,
    tags=['a-korenevich-1']
) as dag:
    t0 = PythonOperator(
        task_id = 'start_dummy_operator',
        python_callable=dummy
    )

    t1 = BranchPythonOperator(
        task_id = 'check_branch_task_id',
        python_callable=check_branch
    )

    t2 = PythonOperator(
        task_id = 'startml_desc',
        python_callable=print_if_startml
    )

    t3 = PythonOperator(
        task_id = 'not_startml_desc',
        python_callable=print_if_not_startml
    )

    t4 = PythonOperator(
        task_id = 'finish_dummy_operator',
        python_callable=dummy
    )

    t0 >> t1

    t2 >> t4
    t3 >> t4