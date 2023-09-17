"""
Test docs
"""
from datetime import datetime, timedelta
from textwrap import dedent

from airflow import DAG

from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
with DAG(
    'hw_arse-beljaev_2',
    default_args={
    'depends_on_past': False,
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    }
    description='hw_2_lesson_11',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2023,17,9),
    catchup=False,
    tags=['example'],
) as dag:
    
    def print_context(ds, **kwargs):
        """PythonOperator"""
        print(kwargs)
        print(ds)
        return 'Whatever you return gets printed in the logs'

    t1 = PythonOperator(
        task_id='print_ds',
        python_callable=print_context,
    )
    
    t2 = BashOperator(
        task_id='execute_pwd',
        bash_command='pwd',
    )
