"""
step_2 DAG
"""
from datetime import datetime, timedelta
from textwrap import dedent

from airflow import DAG

from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator


with DAG(
    'hw_r-shahvaly_2',

    default_args={
        'depends_on_past': False,
        'email': ['airflow@example.com'],
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 1,
        'retry_delay': timedelta(minutes=5),
    },
    description='DAG for step_2',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2023, 1, 1),
    catchup=False,
    tags=['r-shahvaly'],
) as dag:

    def print_ds(ds, **kwargs):
        print(ds)

    t1 = BashOperator(
        task_id='bash_pwd',
        bash_command='pwd',
    )

    t2 = PythonOperator(
        task_id='python_print',
        python_callable=print_ds
    )

    t1 >> t2

