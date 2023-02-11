from datetime import datetime, timedelta
from textwrap import dedent
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

def print_airflow_date(ds, **kwargs):
    print(ds)

with DAG(
    'e-dm_lesson_11_task_2',
    default_args={
        'depends_on_past': False,
        'email': ['airflow@example.com'],
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 1,
        'retry_delay': timedelta(minutes=5),  # timedelta из пакета datetime
    },
    description='My first training DAG',
    start_date=datetime(2023, 2, 11),
    schedule_interval=timedelta(days=1),
    catchup=False,
    tags=['e-dm']
) as dag:
    t1 = BashOperator(
        task_id='bash_pwd',
        bash_command='pwd'
    )

    t2 = PythonOperator(
        task_id='print_airflow_date',
        python_callable=print_airflow_date
    )

    t1 >> t2