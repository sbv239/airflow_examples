"""
DAG 6
"""
from datetime import datetime, timedelta
from textwrap import dedent

from airflow import DAG

from airflow.operators.bash import BashOperator
from airflow.operators.python_operator import PythonOperator

with DAG(
    'senkovskiy_dag6',

    default_args={
        'depends_on_past': False,
        'email': ['airflow@example.com'],
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 1,
        'retry_delay': timedelta(minutes=5),  
    },

    description='Second DAG',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2022, 5, 1),
    catchup=False,
    tags=['senkovskiy_dag6'],

) as dag:

    for i in range(10):

        task1 = BashOperator(
            task_id=f'print_echo_{i}',  
            env={"NUMBER": str(i)},
            bash_command=f"echo $NUMBER",  
        )

    def print_task_number(ts, run_id, **kwargs):
        task_number = kwargs['task_number']
        print(f'task number is: {task_number}')
        print(f'{ts}')
        print(f'{run_id}')

    for i in range(20):
        
        task2 = PythonOperator(
            task_id=f'print_state_{i}', 
            python_callable=print_task_number,
            op_kwargs={'task_number': i}
        )

