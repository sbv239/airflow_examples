"""
my documentation
"""

from datetime import datetime, timedelta
from textwrap import dedent

from airflow import DAG

from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator


def print_ds(ds):
    print(ds)
    return None

with DAG(
    'unit11-2-dementev',
        default_args={
            'depends_on_past': False,
            'email': ['airflow@example.com'],
            'email_on_failure': False,
            'email_on_retry': False,
            'retries': 1,
            'retry_delay': timedelta(minutes=5),  # timedelta из пакета datetime
        },




    description='DAG unit 11-2',
        schedule_interval=timedelta(days=1),
        start_date=datetime(2023, 1, 1),
        catchup=False,



) as dag:
    t1 = BashOperator(
        task_id='pwd',
        bash_command='pwd',
    )
    t2 = PythonOperator(
        task_id = 'printds',
        python_callable=print_ds,
    )

    t2 >> t1



