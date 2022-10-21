from datetime import datetime, timedelta
from textwrap import dedent
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

with DAG(
    'a-buzmakov-13_task_2',
    default_args={
    'depends_on_past': False,
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    }) as dag:
    a1=BashOperator(
        task_id='num2',
        bash_command='pwd')
    def print_lol(ds,**kwargs):
        print(ds)
        return 'lol'
    a2=PythonOperator(
        task_id='print_ret'
        python_callable=print_lol)
    a1 >> a2
        
    
