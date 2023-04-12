from datetime import datetime, timedelta
from textwrap import dedent
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import BranchPythonOperator, PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresHook
from psycopg2.extras import RealDictCursor
from airflow.models import Variable
from airflow.operators.dummy import DummyOperator


def is_ml():
    if Variable.get('is_startml') == 'True':
        return 'startml_desc'
    else:
        return 'not_startml_desc'

def print_true():
    print('StartML is a starter course for ambitious people')

def print_false():
    print('Not a startML course, sorry')




with DAG(
    'HW_13_v-patrakeev',
    default_args={
        'depends_on_past': False,
        'email': ['airflow@example.com'],
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 1,
        'retry_delay': timedelta(minutes=5),  # timedelta из пакета datetime
    },
    start_date=datetime.now(),
) as dag:

    t1 = DummyOperator(task_id='before')

    t2 = BranchPythonOperator(
        task_id='branch_is_ml',
        python_callable=is_ml
    )

    t3 = PythonOperator(task_id='startml_desc', python_callable=print_true)
    t4 = PythonOperator(task_id='not_startml_desc', python_callable=print_false)

    t5 = DummyOperator(task_id='fin')

    t1 >> t2 >> [t3, t4] >> t5