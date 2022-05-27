"""
DAG 12
"""
from datetime import datetime, timedelta
from textwrap import dedent

from airflow import DAG

from airflow.operators.bash import BashOperator
from airflow.operators.python_operator import PythonOperator

from airflow.operators.python import BranchPythonOperator
from airflow.operators.dummy import DummyOperator

with DAG(
    'senkovskiy_dag12',

    default_args={
        'depends_on_past': False,
        'email': ['airflow@example.com'],
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 1,
        'retry_delay': timedelta(minutes=5),  
    },

    description='12th DAG',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2022, 5, 1),
    catchup=False,
    tags=['senkovskiy_dag12'],

) as dag:

    def chose_branch(**kwargs):
        from airflow.models import Variable
        variable = Variable.get("is_startml")
        if variable == "True":
            return 'startml_desc'
        return 'not_startml_desc'

    def func1():
        print("StartML is a starter course for ambitious people")

    def func2():
        print("Not a startML course, sorry")

    before = DummyOperator(
        task_id=f'before_branching' 
    )

    chose = BranchPythonOperator(
        task_id=f'determine_course',
        python_callable=chose_branch,
        trigger_rule="one_succsess",
    )

    t1 = PythonOperator(
        task_id=f'startml_desc', 
        python_callable=func1
    )

    t2 = PythonOperator(
        task_id=f'not_startml_desc', 
        python_callable=func2
    )

    after = DummyOperator(
        task_id=f'after_branching'
    )

before >> chose >> [t1, t2] >> after


