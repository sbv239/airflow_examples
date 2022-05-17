from datetime import datetime, timedelta
from textwrap import dedent
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python_operator import PythonOperator

with DAG(
    'task_01',
    default_args={
        'depends_on_past': False,
        'email': ['yevgeniy.demidov@gmail.com'],
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 1,
        'retry_delay': timedelta(minutes=5),
    },
    description='Lesson_11_task_1',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2022, 5, 16),
    catchup=False,
    tags=['e-demidov'],
) as dag:
    bash_task = BashOperator(
        task_id='p',
        bash_command='pwd',
    )
    
    def print_ds(ds, **kwarg):
        print(ds)
        return 'Любое другое сообщение'

    py_task = PythonOperator(
        task_id='print_ds',
        python_callable=print_ds,
    )

    bash_task >> py_task