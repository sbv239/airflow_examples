from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

def print_task_number(task_number, ts, run_id, **kwargs):
    print(f'task number is: {task_number},\ntime: {ts},\nrun_id: {run_id}.')

with DAG(
    'hw_m-lebedev_7',
    default_args={
        'depends_on_past': False,
        'email': ['airflow@example.com'],
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 1,
        'retry_delay': timedelta(minutes=5),
    },
    description='Homework: 7, login: m-lebedev',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2023, 7, 23),
    catchup=False,
    tags=['m-lebedev'],
) as dag:

    for i in range(1, 11):
        bash_task = BashOperator(
            task_id= f'bash_print_task_{i}',
            bash_command=f'echo {i}',
        )
        
    for i in range(11, 31):
        puthon_task =  PythonOperator(
            task_id= f'python_print_task_{i}',
            python_callable=print_task_number,
            op_kwargs={
                'task_number': i,
                'ts': '{{ ts }}',
                'run_id': '{{ run_id }}'
            }
        )