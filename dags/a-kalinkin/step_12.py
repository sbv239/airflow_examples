from datetime import datetime, timedelta
from textwrap import dedent

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python_operator import PythonOperator
from textwrap import dedent
from airflow.providers.postgres.operators.postgres import PostgresHook

with DAG(
    # !!!!!!!!!!!!!!!!!!!!!!!!!!!
    'hw_12_a-kalinkin',#МЕНЯЙ ИМЯ ДАГА
    # !!!!!!!!!!!!!!!!!!!!!!!!!!!
    default_args={
        'depends_on_past': False,
        'email': ['airflow@example.com'],
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 1,
        'retry_delay': timedelta(minutes=5),
    },

    description='DAG wiht connection',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2022, 1, 1),
    catchup=False,
    tags=['hw_a-kalinkin'],
) as dag:

        def get_variable():
                from airflow.models import Variable
                is_startml = Variable.get("is_startml")
                print(is_startml)


        first = PythonOperator(
                task_id='return_sql_request',
                python_callable=get_variable,
        )

        first
