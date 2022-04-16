"""
Test documentation
"""
from datetime import datetime, timedelta
from textwrap import dedent

from airflow import DAG


from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
with DAG(
    'tutorial',

    default_args={
        'depends_on_past': False,
        'email': ['airflow@example.com'],
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 1,
        'retry_delay': timedelta(minutes=5),
    },
    description='hw_1_khaletina',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2022, 1, 1),
    catchup=False,
    tags=['hw_1_khaletina'],
) as dag:

    t1 = BashOperator(
        task_id='print_pwd',
        bash_command='pwd',
    )

    def print_else(ds, **kwarg):
        print(ds)
        return None

    t2 = PythonOperator(
        task_id='print_else',
        python_callable=print_else,
    )

    t1 >> t2
