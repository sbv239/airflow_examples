from airflow import DAG
from airflow.operators.python_operator import BranchPythonOperator
from airflow.operators.python_operator import PythonOperator
from airflow.operators.dummy import DummyOperator

from airflow.models import Variable
from datetime import datetime, timedelta

with DAG(

    'task_12_breus',

    default_args={

    'depends_on_past': False,
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),  
    'retry_delay': timedelta(minutes=5), 
    },

    schedule_interval=timedelta(days=1),
    start_date=datetime(2022, 3, 31),
    catchup=False
) as dag:

    def branch():

        is_startml = Variable.get("is_startml")

        if is_startml:
            return 'startml_desc'
        else:
            return 'not_startml_desc'

    def start_ml_print():
        print('StartML is a starter course for ambitious people')

    def not_startml_print():
        print('Not a startML course, sorry')


    begin_dummy = DummyOperator(
        task_id = 'before_branching'
    )
            
    determine_course = BranchPythonOperator(
        task_id = 'determine_course',
        python_callable=branch
    )   

    is_startml = PythonOperator(
        task_id = 'startml_desc',
        python_callable=start_ml_print

    )

    not_startml = PythonOperator(
        task_id = 'not_startml_desc',
        python_callable=not_startml_print
    )

    end_dummy = DummyOperator(
        task_id = 'after_branching'
    )

    begin_dummy >> determine_course >> [is_startml, not_startml] >> end_dummy
