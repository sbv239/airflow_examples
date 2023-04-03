from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

with DAG(
    #taskid
    'hw1_task'
    default_args={
        'depends_on_past': False,
        'email': ['airflow@example.com'],
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 1,
        'retry_delay': timedelta(minutes=5),
    },
    description='A simple tutorial DAG',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2023, 1, 1),
    catchup=False,
    tags['hw'],    
    ) as dag:
        t1 = BashOperator(
            taskid='location',
            bash_command='pwd'
            )
        
        def current_date(ds, **kwargs):
            print(ds)
            returm 'first DAG with python operator'
            
        t2 = PythonOperator(
            taskid='current_date',
            python_callable=current_date,
            )
            
        t1 >> t2
