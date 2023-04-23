from datetime import datetime, timedelta

from airflow import DAG
from airflow.models import Variable
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.dummy import DummyOperator


def determinator():
    value = Variable.get("is_startml")
    if value == 'True':
        return 'startml_desc'
    else:
        return 'not_startml_desc'


def startml_handler():
    print('StartML is a starter course for ambitious people')


def not_startml_handler():
    print('Not a startML course, sorry')


with DAG(
        'e_13_demets',
        # Параметры по умолчанию для тасок
        default_args={
            'depends_on_past': False,
            'email': ['airflow@example.com'],
            'email_on_failure': False,
            'email_on_retry': False,
            'retries': 1,
            'retry_delay': timedelta(minutes=5),  # timedelta из пакета datetime
        },
        description='A simple tutorial DAG',
        schedule_interval=timedelta(days=1),
        start_date=datetime(2022, 1, 1),
        catchup=False,
        tags=['demets'],
) as dag:
    t0 = DummyOperator(task_id='before_branching')

    t1 = BranchPythonOperator(
        task_id='determine_course',
        python_callable=determinator)

    t2 = PythonOperator(
        task_id='startml_desc',
        python_callable=startml_handler)

    t3 = PythonOperator(
        task_id='not_startml_desc',
        python_callable=not_startml_handler)

    t4 = DummyOperator(task_id='after_branching')

    t0 >> t1 >> [t2, t3] >> t4