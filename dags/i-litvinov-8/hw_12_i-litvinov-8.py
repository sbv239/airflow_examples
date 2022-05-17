from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python_operator import PythonOperator
from datetime import date, timedelta, datetime
from textwrap import dedent
from psycopg2.extras import RealDictCursor
from airflow.operators.dummy import DummyOperator
from airflow.operators.python import BranchPythonOperator


def get_variable():
    from airflow.models import Variable
    is_startml = Variable.get('is_startml')
    if is_startml == 'True':
        return 'startml_desc'
    return 'not_startml_desc'


def not_startml():
    print("Not a startML course, sorry")


def startml():
    print("StartML is a starter course for ambitious people")


with DAG(
        'hw_12_i-litvinov-8',
        default_args={
            'depends_on_past': False,
            'email': ['airflow@example.com'],
            'email_on_failure': False,
            'email_on_retry': False,
            'retries': 1,
            'retry_delay': timedelta(minutes=5),
        },
        description='hw_12_i-litvinov-8',
        schedule_interval=timedelta(days=1),
        start_date=datetime(2022, 5, 16),
        catchup=False,
        tags=['i-litvinov-8']
) as dag:
    t1 = DummyOperator(
        task_id='before_branching'
    )
    t3 = PythonOperator(
        task_id='not_startml_desc',
        python_callable=not_startml
    )
    t4 = PythonOperator(
        task_id='startml_desc',
        python_callable=startml
    )
    t5 = DummyOperator(
        task_id='after_branching'
    )

    t2 = BranchPythonOperator(
        task_id='branching',
        python_callable=get_variable,
    )

    t1 >> t2 >> [t3, t4] >> t5

