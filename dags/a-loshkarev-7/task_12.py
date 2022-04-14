from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.models import Variable


def branching_func():
    var = Variable.get('is_startml')

    if var:
        return 'startml_desc'

    return 'not_startml_desc'

def task_1():
    print('StartML is a starter course for ambitious people')

def task_2():
    print('Not a startML course, sorry')

with DAG(
    'aloshkarev_task_12',
    default_args = {
        'depends_on_past': False,
        'email': ['airflow@example.com'],
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 1,
        'retry_delay': timedelta(minutes=5),  # timedelta из пакета datetime
    },
    schedule_interval=timedelta(days=1),
    start_date=datetime(2022, 4, 12),
    catchup=False,
) as dag:
    branching = BranchPythonOperator(
            task_id='branch_task',
            python_callable=branching_func
        )

    t1 = PythonOperator(
            task_id='startml_desc',
            python_callable=task_1
        )

    t2 = PythonOperator(
            task_id='not_startml_desc',
            python_callable=task_2
        )

    branching >> [t1, t2]
