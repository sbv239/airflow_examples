from datetime import datetime, timedelta
from airflow import DAG

from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.dummy import DummyOperator
from airflow.models import Variable

def startml_true():
    print("StartML is a starter course for ambitious people")

def startml_false():
    print("Not a startML course, sorry")

def get_branch(**kwargs):
    if Variable.get('is_startml') == 'True':
        return 'startml_True'
    else:
        return 'startml_False'


with DAG(
    'a-igumnov_task_13',

    default_args={
    'depends_on_past': False,
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),  # timedelta из пакета datetime
    },
    description = 'hw_13_a-igumnov',
    schedule_interval = timedelta(days=1),
    start_date=datetime(2023, 1, 1),
    catchup=False,
    tags=['hw_13_a-igumnov_Branching']


) as dag:

    start = DummyOperator(
        task_id = 'start'
    )

    decision = BranchPythonOperator(
        task_id = 'is_startml',
        python_callable = get_branch
    )

    is_startml = PythonOperator(
        task_id = 'startml_True',
        python_callable = startml_true
    )

    not_startml = PythonOperator(
        task_id = 'startml_False',
        python_callable = startml_false
    )

    end = DummyOperator(
        task_id = 'end'
    )

    start >> decision >> [is_startml, not_startml] >> end