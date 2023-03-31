from airflow import DAG
from airflow.models import Variable
from airflow.operators.python import BranchPythonOperator, PythonOperator
from airflow.operators.dummy_operator import DummyOperator
from datetime import datetime, timedelta


def choose_branch():
    if Variable.get('is_startml') == 'True':
        return 'startml_desc'
    return 'not_startml_desc'


def print_start_ml():
    print('StartML is a starter course for ambitious people')


def print_not_start_ml():
    print('Not a startML course, sorry')


with DAG(
    'hw_13',
    default_args={
        'depends_on_past': False,
        'email': ['airflow@example.com'],
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 1,
        'retry_delay': timedelta(minutes=5),  # timedelta из пакета datetime
    },
    description='hw_13',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2023, 3, 31),
    catchup=False,
    tags=['hw_13'],
) as dag:

    dummy_first = DummyOperator(
        task_id='dummy_first'
    )

    branch = BranchPythonOperator(
        task_id='branching',
        python_callable=choose_branch
    )

    startml_desc = PythonOperator(
        task_id='startml_desc',
        python_callable=print_start_ml
    )

    not_startml_desc = PythonOperator(
        task_id='not_startml_desc',
        python_callable=print_not_start_ml
    )

    dummy_last = DummyOperator(
        task_id='dummy_last'
    )

    dummy_first >> branch >> [startml_desc, not_startml_desc] >> dummy_last
