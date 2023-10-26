from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta

def print_ds(ds = None):
    print(ds)

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delta': timedelta(minutes=5)
}

with DAG(
    'hw_2_a-buldakov',
    start_date=datetime.now(),
    default_args=default_args,
    tags=['a-buldakov']
) as dag:
    
    t1 = BashOperator(
        task_id = 'current_location',
        bash_command = 'pwd'
    )
    
    t2 = PythonOperator(
        task_id = 'print_ds',
        python_callable = print_ds
    )
    
    t1 >> t2