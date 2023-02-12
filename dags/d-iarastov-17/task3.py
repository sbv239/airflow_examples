from datetime import datetime, timedelta
from airflow import DAG
from textwrap import dedent

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
    'hw_3_d-iarastov-17',
    default_args = default_args,
    start_date=datetime(2023, 1, 1),
    description = "For loop in tasks iteration"
) as dag:
    for i in range(30):
        if i <=10:
            t1 = BashOperator(
                task_id='print_in_bash_' + str(i),
                bash_command=f"echo line number {i}"
                )
        else:
            def print_task_number(task_number, **kwargs):
                print(f'task number is: {task_number}')
                print(kwargs)
            t2 = PythonOperator(
                task_id='task_number_' + str(i),
                python_callable=print_task_number,
                op_kwargs={'task_number': i}
                )
    t1>>t2


