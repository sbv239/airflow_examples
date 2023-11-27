from airflow import DAG

from datetime import timedelta, datetime

from airflow.operators.python import BranchPythonOperator, PythonOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.models import Variable


def condition():
    task_id = Variable.get("is_startml")
    print(task_id)
    if task_id:
        return 'startml_desc'
    else:
        return 'not_startml_desc'


def print_true():
    print("StartML is a starter course for ambitious people")


def print_false():
    print("Not a startML course, sorry")


with DAG(
    'hw_d-trubitsin_12',

    default_args={
        'depends_on_past': False,
        'email': ['airflow@example.com'],
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 1,
        'retry_delay': timedelta(minutes=5),  # timedelta из пакета datetime
    },

    description='First DAG',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2023, 11, 26),
    catchup=False,
    tags=['d-trubitsin_12'],
) as dag:

    d1 = DummyOperator(
        task_id='dummy_1',
    )

    branching = BranchPythonOperator(
        task_id='branching',
        python_callable=condition,
    )
    t1 = PythonOperator(
        task_id='startml_desc',
        python_callable=print_true,
    )
    t2 = PythonOperator(
        task_id='not_startml_desc',
        python_callable=print_false,
    )
    d2 = DummyOperator(
        task_id='dummy_2',
    )

    d1 >> branching >> [t1, t2] >> d2
