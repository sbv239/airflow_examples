from datetime import timedelta, datetime
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from textwrap import dedent
from airflow.models import Variable
from airflow.hooks.base_hook import BaseHook
from psycopg2.extras import RealDictCursor
from airflow import DAG
from airflow.providers.postgres.operators.postgres import PostgresHook


def get_variable():
    print(Variable.get("is_startml"))


default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}

with DAG(
    'hw_12_de-jakovlev',
    start_date=datetime(2021, 1, 1),
    max_active_runs=2,
    schedule_interval=timedelta(minutes=30),
    default_args=default_args,
    catchup=False
) as dag:

    p1 = PythonOperator(
        task_id='variable',
        python_callable=get_variable,
    )









