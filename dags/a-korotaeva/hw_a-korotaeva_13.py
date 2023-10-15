from datetime import datetime, timedelta
from textwrap import dedent
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.models import Variable
from airflow.operators.dummy_operator import DummyOperator


def _choose_best_model():
  if Variable.get("is_startml") == "True":
    return 'startml_desc'
  return 'not_startml_desc'

def true_print():
    print('StartML is a starter course for ambitious people')

def false_print():
    print('Not a startML course, sorry')

with DAG(
    'hw_13_a-korotaeva',
    default_args={
        'depends_on_past': False,
        'email': ['airflow@example.com'],
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 1,
        'retry_delay': timedelta(minutes=5),
    },
    description='A simple tutorial DAG', schedule_interval=timedelta(days=1),
    start_date=datetime(2022, 10, 13),
    catchup=False
) as dag:

    t1 = DummyOperator(task_id = 'before_branching')
    t2 = BranchPythonOperator(task_id='branch', python_callable=_choose_best_model)
    t4 = PythonOperator(task_id='startml_desc', python_callable=true_print)
    t5 = PythonOperator(task_id='not_startml_desc', python_callable=false_print)
    t3 = DummyOperator(task_id='after_branching')

    t1 >> t2 >> [t4, t5] >> t3
