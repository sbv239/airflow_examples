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
    'hw_d-ajmuratov-26_3',
    default_args=default_args,
    start_date=datetime.now(),
    catchup=False,
    tags=['HM3']
) as dag:
    
    def print_start(ds):
        print('3.2.1...start!!!')
    run_this = PythonOperator(
        task_id='start',
        provide_context=True,
        python_callable=print_start
    )

    def print_curr_task_number(**kwargs):
        print(f"task number is: {kwargs['task_number']}")
    for i in range(1, 11):
        t = BashOperator(
            task_id=f'echo_curr_task_number_{i}',
            bash_command=f'echo {i}'
        )
        run_this >> t
    for i in range(11, 31):
        t = PythonOperator(
            task_id=f'print_curr_task_number_{i}',
            python_callable=print_curr_task_number,
            provide_context=True,
            op_kwargs={'task_number': i}
        )
        run_this >> t

    run_this