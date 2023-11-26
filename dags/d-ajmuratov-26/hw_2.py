"""
Hello, Airflow Documentation!!!
"""
from textwrap import dedent

from datetime import timedelta
from datetime import datetime

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

default_args={
    'depends_on_past': False,
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),  # timedelta из пакета datetime
}
with DAG(
    'hw_d-ajmuratov-26_2',
    default_args=default_args,
    description='Home Work 2',
    start_date=datetime(2023, 11, 26),
    catchup=False,
    tags=['Home Work 2']
) as dag:
    t1 = BashOperator(
        task_id='print_curr_dir',
        bash_command='pwd'
    )

    def print_df(ds, **kwargs):
        print('Hello, Airflow!!!')
        print(**kwargs)
        print(ds)
    t2 = PythonOperator(
        task_id='print_ds',
        python_callable=print_df
    )

    t1 >> t2