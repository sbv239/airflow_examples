"""
Task 1: https://lab.karpov.courses/learning/84/module/1049/lesson/10040/29383/139286/
"""
from datetime import datetime, timedelta
from textwrap import dedent

from airflow import DAG

from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator


with DAG(
    'hw_1_s.zlenko-7',
    default_args={
    'depends_on_past': False,
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    },
    schedule_interval=timedelta(days=1),
    start_date=datetime(2022, 1, 1),
    catchup=False,
) as dag:

    t1 = BashOperator(
        task_id='print_current_directory',
        bash_command='pwd'
    )


    def print_ds(ds):
        print(ds)

    t2 = PythonOperator(
    task_id='print_ds',
    python_callable=print_ds,
)

    t1 >> t2