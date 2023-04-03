from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.dummy import DummyOperator
from airflow.models import Variable


def branch_func(**kwargs):
    is_startml = Variable.get("is_startml")
    if(is_startml == "True"):
        return 'startml_desc'
    else:
        return 'not_startml_desc'

def print_startml():
    print("StartML is a starter course for ambitious people")

def print_not_startml():
    print("Not a startML course, sorry")
    
    
with DAG(
    'tasks',
    description='Branching DAG',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2023, 3, 31),
    catchup=False,
    tags=['task13'],
) as dag:
    
    begin = DummyOperator(
        task_id="begin_branching",
    )

    branch_func = BranchPythonOperator(
        task_id='determine_course',
        python_callable=branch_func
    )
    
    startml_desc = PythonOperator(
        task_id='startml_desc',
        python_callable=print_startml
    )
    
    not_startml_desc = PythonOperator(
        task_id='not_startml_desc',
        python_callable=print_not_startml
    )
    
    end = DummyOperator(
        task_id="end_branching",
    )
    
    begin >> branch_func >> [startml_desc, not_startml_desc] >> end
    
