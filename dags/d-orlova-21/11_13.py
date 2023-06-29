from airflow import DAG
from datetime import datetime, timedelta
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.models import Variable
from airflow.operators.dummy import DummyOperator

def variable():
    variable = Variable.get('is_startml')
    if variable ==  'True':
        return 'startml_desc'
    else:
        return 'not_startml_desc'

def start_ml():
    print ('StartML is a starter course for ambitious people')

def not_start_ml():
    print('Not a startML course, sorry')

with DAG (
    'hw_d-orlova-21_13',
    default_args={
        'depends_on_past': False,
        'email': ['airflow@example.com'],
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 1,
        'retry_delay': timedelta(minutes=5),  # timedelta из пакета datetime
    },
    description = 'dag for lesson 11.13',
    schedule_interval = timedelta(days=1),
    start_date=datetime(2023, 6, 28),
    catchup = False,
    tags = ['HW13']
) as dag:

    dummy_start = DummyOperator(task_id = 'before_branching')

    determine_course = BranchPythonOperator(
        task_id = 'determine_course',
        python_callable = variable,
    )

    task_1 = PythonOperator(
        task_id = 'startml_desc',
        python_callable = start_ml,
    )

    task_2 = PythonOperator(
        task_id = 'not_startml_desc',
        python_callable = not_start_ml,
    )

    dummy_finish = DummyOperator(task_id = 'after_branching')

    dummy_start >> determine_course >> [task_1, task_2] >> dummy_finish