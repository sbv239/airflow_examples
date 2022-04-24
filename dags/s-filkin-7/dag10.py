from datetime import datetime, timedelta
from textwrap import dedent

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.models import Variable


def python_func1():
    is_startml = Variable.get("is_startml")
    print(is_startml)
    return 'ok'


with DAG(
    's-filkin-7-dag10',
    default_args={
        'depends_on_past': False,
        'email': ['airflow@example.com'],
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 1,
        'retry_delay': timedelta(minutes=5),
    },
    schedule_interval=timedelta(days=1),
    start_date=datetime(2022, 4, 23),
) as dag:

    t1 = PythonOperator(
        task_id='t1', 
        python_callable=python_func1,

    )

    t1