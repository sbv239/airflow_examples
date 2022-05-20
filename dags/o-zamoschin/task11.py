"""
Start-ml Airflow Task 11
"""
from datetime import datetime, timedelta

from airflow import DAG

from airflow.operators.python import PythonOperator
from airflow.models import Variable

def print_var():
    is_startml = Variable.get("is_startml")
    print(is_startml)

default_args={
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    'hw_11_o-zamoschin',
    default_args=default_args,
    schedule_interval=timedelta(days=1),
    start_date=datetime(2022, 1, 1),
    catchup=False,
) as dag:
    
    t1 = PythonOperator(
        task_id = 'print_var',
        python_callable=print_var,
    )

    t1