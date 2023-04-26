"""
Test documentation
"""
from datetime import datetime, timedelta
from textwrap import dedent

from airflow import DAG

from airflow.operators.python_operator import PythonOperator
from airflow.operators.bash import BashOperator

from airflow.providers.postgres.operators.postgres import PostgresHook

with DAG(
    'hw_12_a_loskutov',
    # Параметры по умолчанию для тасок

    default_args={
        'depends_on_past': False,
        'email': ['airflow@example.com'],
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 1,
        'retry_delay': timedelta(minutes=5),  # timedelta из пакета datetime
    },

    description='My first DAG',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2023, 4, 25),
    catchup=False,
    tags=['Loskutov_hm'],
) as dag:    

    def get_variable():
            from airflow.models import Variable
            is_startml = Variable.get("is_startml")
            print(is_startml)


    get_variab = PythonOperator(
            task_id='variable',
            python_callable=get_variable,
    )

    get_variab

