from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.dummy import DummyOperator
from textwrap import dedent
from airflow.models import Variable

default_args = {
    'depends_on_past': False,
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),

}
with DAG('hw_d-shestak_12',
         default_args=default_args,
         description='hw_d-shestak_12',
         schedule_interval=timedelta(days=1),
         start_date=datetime(2023, 10, 21),
         tags=['hw_12_d-shestak']
         ) as dag:

    def branching():
        is_startml = Variable.get('is_startml')
        if is_startml == 'True':
            return 'startml_desc'
        else:
            return 'not_startml_desc'

    def print_true():
        print("StartML is a starter course for ambitious people")

    def print_false():
        print('Not a startML course, sorry')

    start_dummy = DummyOperator(
        task_id='start_branching'
    )

    branch = BranchPythonOperator(
        task_id='which_course',
        python_callable=branching
    )

    true = PythonOperator(
        task_id='startml_desc',
        python_callable=print_true
    )

    false = PythonOperator(
        task_id='not_startml_desc',
        python_callable=print_false
    )

    end_dummy = DummyOperator(
        task_id='end_branching'
    )

    start_dummy >> branch >> [true, false] >> end_dummy