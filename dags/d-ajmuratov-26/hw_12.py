from datetime import datetime, timedelta
from textwrap import dedent

from airflow import DAG

from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

with DAG(
    'hw_d-ajmuratov-26_11',
    default_args={
        'depends_on_past': False,
        'email': ['airflow@example.com'],
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 1,
        'retry_delay': timedelta(minutes=5)
        },
    schedule_interval=timedelta(days=1),
    start_date=datetime.now(),
    catchup=False,
    tags=['HW 12']
) as dag:

    def get_variable():
        from airflow.models import Variable
        is_startml = Variable.get('is_startml')
        print(is_startml)
    t = PythonOperator(
        task_id='get_variable',
        python_callable=get_variable
    )
    t