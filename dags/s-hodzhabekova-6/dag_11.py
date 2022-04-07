from datetime import datetime, timedelta
from textwrap import dedent
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresHook


def get_variable():
    from airflow.models import Variable
    is_startml = Variable.get("is_startml")
    print(f"is_startml variable: {is_startml}")
              


with DAG(
    'hw_11_s-hodzhabekova-6',
    default_args={
        'depends_on_past': False,
        'email': ['airflow@example.com'],
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 1,
        'retry_delay': timedelta(minutes=5),
    },
    description='A simple tutorial DAG',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2022, 1, 1),
    catchup=False,
    tags=['hw_11', 'khodjabekova'],
) as dag:

    task = PythonOperator(
        task_id='get_variable',
        python_callable=get_variable,
    )

    task
