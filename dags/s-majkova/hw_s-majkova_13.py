from datetime import datetime, timedelta
from textwrap import dedent
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import BranchPythonOperator, PythonOperator
from airflow.models import Variable


with DAG(
    'hw_s-majkova_13',
    default_args = {
    'depens_on_past': False,
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes = 5),
    },
    description = 'first_dynamic_DAG',
    schedule_interval=timedelta(days=1),
    start_date = datetime(2023, 11, 26),
    catchup = False,
    tags = ['hw_13'],
) as dag:
    def choose_branch():
        if Variable.get('is_startml') == 'True':
            return 'startml_desc'
        else:
            return 'not_startml_desc'


    def startml_desc():
        print('StartML is a starter course for ambitious people')


    def not_startml_desc():
        print('Not a startML course, sorry')
    t1 = BranchPythonOperator(
        task_id = 'branching',
        python_callable = choose_branch
    )
    t2 = PythonOperator(
        task_id = 'startml_desc',
        python_callable = startml_desc
    )
    t3 = PythonOperator(
        task_id = 'not_startml_desc',
        python_callable = not_startml_desc
    )
t1 >> t2
t1 >> t3