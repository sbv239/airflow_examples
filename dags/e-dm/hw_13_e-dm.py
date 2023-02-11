from airflow import DAG
from airflow.operators.python_operator import PythonOperator, BranchPythonOperator
from airflow.operators.dummy import DummyOperator
from datetime import datetime, timedelta
from airflow.models import Variable

def startml_desc():
    print('StartML is a starter course for ambitious people')

def not_startml_desc():
    print('Not a startML course, sorry')

def determine_course():
    if Variable.get("is_startml") == 'True':
        return 'startml_desc'
    return 'not_startml_desc'

with DAG(
    'hw_13_e-dm_dag',
    default_args = {
        'depends_on_past': False,
        'email': ['airflow@example.com'],
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 1,
        'retry_delay': timedelta(minutes=5),
        },
        description='lesson_11_task_13',
        start_date=datetime(2023, 2, 11),
        catchup=False,
        tags=['hw_13_e-dm_tag'],
) as dag:

    before_branching= DummyOperator(
        task_id='before_branching'
    )

    determine_course = BranchPythonOperator(
        task_id = 'determine_course',
        python_callable = determine_course
    )

    startml_desc = PythonOperator(
        task_id = 'startml_desc',
        python_callable = startml_desc
    )

    not_startml_desc = PythonOperator(
        task_id = 'not_startml_desc',
        python_callable = not_startml_desc
    )	

    after_branching = DummyOperator(
        task_id = 'after_branching',
        trigger_rule = 'none_failed_or_skipped'
    )
	# Последовательность задач:
    # before_branching >> determine_course
    before_branching >> determine_course >> [startml_desc, not_startml_desc] >> after_branching