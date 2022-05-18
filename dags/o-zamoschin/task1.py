"""
Start-ml Airflow Task 1
"""
from datetime import datetime, timedelta
from textwrap import dedent

from airflow import DAG

from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

def print_ds(ds):
    print(ds)
    return 'I return this string'

default_args={
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    'hw_1_o-zamoschin',
    default_args=default_args,
    schedule_interval=timedelta(days=1),
    start_date=datetime(2022, 1, 1),
    catchup=False,
) as dag:

    t1 = BashOperator(
        task_id='print_directory',
        bash_command='pwd',
    )

    t2 = PythonOperator(
    task_id='print_the_ds',
    python_callable=print_ds,
)

    t1 >> t2