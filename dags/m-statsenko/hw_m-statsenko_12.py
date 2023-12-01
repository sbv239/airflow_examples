from datetime import datetime, timedelta
from textwrap import dedent
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.models import Variable

 # необходимо передать имя, заданное при создании Variable


def get_variable():
    print("is_startml:",Variable.get("is_startml") )

default_args={
        'depends_on_past': False,
        'email': ['airflow@example.com'],
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 1,
        'retry_delay': timedelta(minutes=5),
}

with DAG(
    'hw_12_m-statsenko',
    default_args=default_args,
    description='HW 12',
    start_date=datetime(2023, 11, 30),
    catchup=False,
    tags=['HW 12']
) as dag:
        t1 = PythonOperator(
            task_id='get_variable',
            python_callable=get_variable
        )
