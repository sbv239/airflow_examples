from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.dummy import DummyOperator


def get_variable():
    from airflow.models import Variable
    is_startml = Variable.get('is_startml')
    print (is_startml)

def decide_which_path():
    if bool(is_startml):
        return 'startml_desc'
    if not bool(is_startml):
        return 'not_startml_desc'

def startml_course():
    print('StartML is a starter course for ambitious people')

def not_startml_course():
    print('Not a startML course, sorry')

with DAG(
    'hw_13_a-vahterkina',
    default_args={
        'depends_on_past': False,
        'email': ['airflow@example.com'],
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 1,
        'retry_delay': timedelta(minutes=5),
        },
        description = 'hw_13_a-vahterkina',
        schedule_interval = timedelta(days = 1),
        start_date = datetime(2023, 10, 22),
        catchup = False,
) as dag:

    t1 = PythonOperator(
        task_id = 'print_variable',
        python_callable = get_variable,
    )

    dummy_task_before = DummyOperator(
        task_id = 'determine_course',
    )

    branch_task = BranchPythonOperator(
        task_id = 'path_choosing',
        python_callable = decide_which_path,
    )

    t2 = PythonOperator(
        task_id = 'startml_desc',
        python_callable = startml_course,
    )

    t3 = PythonOperator(
        task_id = 'not_startml_desc',
        python_callable = not_startml_course,
    )

    dummy_task_after = DummyOperator(
        task_id = 'after_branching'
    )


dummy_task_before >> branch_task >> [t2, t3] >> dummy_task_after

