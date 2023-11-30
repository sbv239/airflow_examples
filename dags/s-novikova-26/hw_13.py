"""
HW 13
"""
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.models import Variable
from airflow.operators.python_operator import BranchPythonOperator
from airflow.operators.dummy_operator import DummyOperator


 # необходимо передать имя, заданное при создании Variable


def determine_course():
    if Variable.get('is_startml') == 'True':
        return t3.task_id
    return t4.task_id

def startml_desc():
     print("StartML is a starter course for ambitious people")

def not_startml_desc():
     print("Not a startML course, sorry")

default_args={
        'depends_on_past': False,
        'email': ['airflow@example.com'],
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 1,
        'retry_delay': timedelta(minutes=5),
}

with DAG(
    'hw_13_s-novikova-26',
    default_args=default_args,
    description='HW 13',
    start_date=datetime(2023, 11, 30),
    catchup=False,
    tags=['HW 13']
) as dag:
        t1 = DummyOperator(
            task_id='before_branching',
            dag=dag,
        )
        t2 = BranchPythonOperator(
              task_id='determine_course',
              python_callable=determine_course
              )
        t3 = PythonOperator(
              task_id='startml_desc',
              python_callable=startml_desc
              )
        t4 = PythonOperator(
              task_id='not_startml_desc',
              python_callable=not_startml_desc
              )
        t5 = DummyOperator(
            task_id='after_branching',
            dag=dag,
        )