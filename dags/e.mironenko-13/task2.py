from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python_operator import PythonOperator

from datetime import timedelta, datetime

with DAG(
    'hw_e.mironenko-13_2',
    default_args={
        'depends_on_past': False,
        'email': ['airflow@example.com'],
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 1,
        'retry_delay': timedelta(minutes=5),  # timedelta из пакета datetime
            },
    start_date=datetime(2023, 6, 26),
    tags = ['e.mironenko-13']
) as dag:
    t1 = BashOperator(
        task_id='task_bash_pwd',
        bash_command='pwd',
    )

    def print_ds(ds):
        print(ds)
        print(f'time: {ds}')

    t2 = PythonOperator(
        task_id='task_python_ds',
        python_callable=print_ds,
    )

t1 >> t2