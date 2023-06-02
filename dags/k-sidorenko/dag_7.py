"""
Test documentation
"""
from datetime import datetime, timedelta
from textwrap import dedent

from airflow import DAG

from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

descr = 'k-sidorenko_Task_7'

def print_context(ds, **kwargs):
    # print(kwargs)
    print(ds)
    return 'Whatever you return gets printed in the logs'

def print_task_number(task_number, ts, run_id, **kwargs):
    print(f'task number is: {task_number}')
    print(ts)
    print(run_id)


default_args={
        'depends_on_past': False,
        'email': ['airflow@example.com'],
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 1,
        'retry_delay': timedelta(minutes=5),  # timedelta из пакета datetime
    }
    
with DAG(
    descr,

    default_args=default_args,
    
    description=descr,
    schedule_interval=timedelta(days=1),
    start_date=datetime(2023, 1, 1),
    catchup=False,
    tags=[descr],
) as dag:

    for i in range(1, 31):
        if i <= 10:
            BashOperator(
                task_id = f'task_{i}',
                bash_command = f'echo_{i}',
            )
        else:
            PythonOperator(
                task_id=f'python_task_{i}',
                python_callable=print_task_number,
                op_kwargs={'task_number': i}
            )
        