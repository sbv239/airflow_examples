from airflow import datetime, timedelta
from textwrap import dedent
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

with DAG(
    'hw',
    default_args={
        'depends_on_past': False,
        'email': ['airflow@example.com'],
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 1,
        'retry_delay': timedelta(minutes=5),  # timedelta из пакета datetime
    },
    description='First DAG start',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2023, 10, 18),
) as dag:

    t1 = BashOperator(
        task_id = 'pwd',
        bash_command = 'pwd'
    )

    def print_ds(ds):
        print(ds)
    
    t2 = PythonOperator(
        task_id = "print ds"
        pythoon_callable = print_ds
    )
    
    t1 >> t2
    