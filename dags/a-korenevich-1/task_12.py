from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta

from airflow.models import Variable


def print_variables():
    is_startml = Variable.get("is_startml")  # необходимо передать имя, заданное при создании Variable
    print(is_startml)

# Default settings applied to all tasks
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}

with DAG(
    'hw_12_a-korenevich-1',
    start_date=datetime(2021, 1, 1),
    max_active_runs=2,
    schedule_interval=timedelta(minutes=30),
    default_args=default_args,
    catchup=False,
    tags=['a-korenevich-1']
) as dag:
    t1 = PythonOperator(
        task_id = 'print_variables_task_id',
        python_callable=print_variables
    )