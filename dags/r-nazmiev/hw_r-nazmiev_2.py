"""
My first DAG
"""
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

with DAG(
    'hw_r-nazmiev_2',
    default_args={
        'depends_on_past': False,
        'email': ['airflow@example.com'],
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 1,
        'retry_delay': timedelta(minutes=5), 
    },
    description='A simple practice DAG',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2023,7,17),
    catchup=False,
    tags=['example'],
) as dag:
    
    def print_context(ds):
        print(ds)

    
    print_ds = PythonOperator(
        task_id='print_ds', 
        python_callable=print_context,
    )

    pwd = BashOperator(
        task_id='print_directory_path',
        bash_command='pwd',
    )

    pwd >> print_ds
