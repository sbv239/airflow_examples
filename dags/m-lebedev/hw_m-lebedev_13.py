from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import BranchPythonOperator
from airflow.models import Variable

def branch():
    is_startml = Variable.get("is_startml")
    if is_startml == 'True':
        return 'startml_desc'
    else:
        return 'not_startml_desc'
    
def task1():
    print("StartML is a starter course for ambitious people")

def task2():
    print("Not a startML course, sorry")
    
with DAG(
    'hw_m-lebedev_13',
    default_args={
        'depends_on_past': False,
        'email': ['airflow@example.com'],
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 1,
        'retry_delay': timedelta(minutes=5),
    },
    description='Homework: 13, login: m-lebedev',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2023, 7, 24),
    catchup=False,
    tags=['m-lebedev'],
) as dag:

    start =  DummyOperator(task_id = 'start_point')
    
    t_branch =  BranchPythonOperator(
        task_id = 'branch_point',
        python_callable=branch,
    )
    
    t1 =  PythonOperator(
        task_id = 'startml_desc',
        python_callable=task1,
    )
    
    t2 =  PythonOperator(
        task_id = 'not_startml_desc',
        python_callable=task2,
    )

    end =  DummyOperator(task_id = 'end_point')
    
    start >> t_branch >> [t1, t2] >> end