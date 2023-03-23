from datetime import datetime, timedelta
from textwrap import dedent

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

with DAG(
    'dag_task2',
    default_args={
    'depends_on_past': False,
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),  # timedelta из пакета datetime
    },
    start_date = datetime(2023, 3, 23)
) as dag:
    t1 = BashOperator(
        task_id = 'print_dir', 
        bash_command = 'pwd'
        )

    def print_ds(ds, **kwargs):
        print(ds)

    t2 = PythonOperator(
        task_id = 'print_ds',
        python_callable = print_ds
    )

    t1 >> t2
