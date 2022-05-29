"""
First dag
"""
from datetime import datetime, timedelta
from textwrap import dedent

from airflow import DAG

from airflow.operators.bash import BashOperator
from airflow.operators.python_operator import PythonOperator
with DAG(
    'first_dag',

    default_args={
        'depends_on_past': False,
        'email': ['airflow@example.com'],
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 1,
        #'retry_delay': timedelta(minutes=5),  
    },

    description='First DAG',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2022, 5, 1),
    catchup=False,
    tags=['first_dag'],

) as dag:

    task1 = BashOperator(
        task_id='print_directory',  
        bash_command='pwd',  
    )

    def print_context(ds, **kwargs):
        print(ds)
        return 'Context is printed'

    task2 = PythonOperator(
        task_id='print_date', 
        python_callable=print_context,
    )

    task1 >> task2