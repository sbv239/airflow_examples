from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

with DAG(
    'hw_m-azizov_1',
    default_args={
        'depends_on_past': False,
        'email': ['airflow@example.com'],
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 1,
        'retry_delay': timedelta(minutes=5),  # timedelta из пакета datetime
    },
    description='A simple tutorial DAG',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2023, 10, 25),
    catchup=False
) as dag:

    def print_ds(ds):
        print(ds)

    t1 = PythonOperator(
        task_id='print_ds',
        python_callable=print_ds
    )

    t2 = BashOperator(
        task_id='pwd_command',
        bash_command='pwd'
    )

    t2 >> t1