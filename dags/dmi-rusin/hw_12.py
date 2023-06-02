from datetime import datetime, timedelta
from textwrap import dedent

from airflow import DAG

from airflow.operators.python import PythonOperator
from airflow.models import Variable


def install_variable():
    is_startml = Variable.get("is_startml")
    print(is_startml)

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}

with DAG(
        'hw_dmi-rusin_12',
        start_date=datetime(2023, 6, 1),
        schedule_interval=timedelta(minutes=5),
        max_active_runs=2,
        default_args=default_args,
        catchup=False
) as dag:
    task_1 = PythonOperator(
        task_id='task_12',
        python_callable=install_variable
    )