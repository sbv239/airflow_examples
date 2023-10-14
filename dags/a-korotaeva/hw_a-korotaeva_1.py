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
    description='A simple tutorial DAG', schedule_interval=timedelta(days=1),
    start_date=datetime(2023, 10, 13),
    catchup=False,
    tags=['example']
) as dag:

    date = "{{ ds }}"
    t1 = BashOperator(task_id='print_current_directory', bash_command='pwd')

    def print_date(ds):
        print(ds)

    t2 = PythonOperator(task_id='print date', python_callable=print_date)

    t1 >> t2
