from datetime import datetime, timedelta
from textwrap import dedent
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta
from airflow.hooks.base import BaseHook
from psycopg2.extras import RealDictCursor
import psycopg2
from airflow.providers.postgres.operators.postgres import PostgresHook
from airflow.models import Variable

with DAG(
    'hw_11_a-haletina-7',
    default_args={
        'depends_on_past': False,
        'email': ['airflow@example.com'],
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 1,
        'retry_delay': timedelta(minutes=5),
    },
    description='hw_11_a-haletina-7',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2022, 4, 11),
    catchup=False,
    tags=['hw_11_a-haletina-7'],
) as dag:

    def start_ml():
        print(Variable.get('is_startml'))

    t1 = PythonOperator(
        task_id='start_ml',
        python_callable=start_ml,
    )

    t1