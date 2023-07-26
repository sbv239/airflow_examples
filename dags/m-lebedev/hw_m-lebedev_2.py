from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

def some_print(ds):
    print(f'current date: {ds}')

with DAG(
    'hw_m-lebedev_2',
    default_args={
        'depends_on_past': False,
        'email': ['airflow@example.com'],
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 1,
        'retry_delay': timedelta(minutes=5),
    },
    description='Homework: 2, login: m-lebedev',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2023, 7, 23),
    catchup=False,
    tags=['m-lebedev'],
) as dag:

    t1 = BashOperator(
        task_id='print_dir',
        bash_command='pwd',
    )

    t2 = PythonOperator(
        task_id='print_date',
         python_callable=some_print,
    )

    t1 >> t2