from datetime import datetime, timedelta
from textwrap import dedent

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python_operator import PythonOperator
from textwrap import dedent
from airflow.providers.postgres.operators.postgres import PostgresHook
from airflow.operators.dummy import DummyOperator
from airflow.operators.python import BranchPythonOperator


with DAG(
    # !!!!!!!!!!!!!!!!!!!!!!!!!!!
    'hw_13_a-kalinkin',#МЕНЯЙ ИМЯ ДАГА
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


        def branch_func():
                from airflow.models import Variable
                is_startml = Variable.get("is_startml")
                if is_startml=="True":
                        return 'startml_desc'
                else:
                        return 'not_startml_desc'

        def task_1():
                print("StartML is a starter course for ambitious people")

        def task_2():
                print("Not a startML course, sorry")

        branch = BranchPythonOperator(
                task_id='return_task',
                python_callable=branch_func
        )

        first = PythonOperator(
                task_id='startml_desc',
                python_callable = task_1,
        )

        second = PythonOperator(
                task_id='not_startml_desc',
                python_callable = task_2,
        )

        start = DummyOperator(
                task_id='start'
        )

        stop = DummyOperator(
                task_id='stop'
        )

        start >> branch >> [first, second] >> stop