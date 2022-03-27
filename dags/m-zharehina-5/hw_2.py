"""
hw_1_m-zharehina-5
"""
from datetime import datetime, timedelta
from textwrap import dedent

from airflow import DAG

from airflow.operators.bash import BashOperator
from airflow.operators.python_operator import PythonOperator


with DAG(
    'hw_2_m_zharehina_5',
    default_args={
        'depends_on_past': False,
        'email': ['airflow@example.com'],
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 1,
        'retry_delay': timedelta(minutes=5), 
    },
    description='hw_2_m_zharehina_5',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2022, 3, 26),
    catchup=False,
    tags=['hw_2_m_zharehina_5'],
    ) as dag:
       
    for i in range(10):
        task_b = BashOperator(
            task_id='echo_' + str(i),
            bash_command=f'echo {i}',
        )

    def print_task_number(task_number):
        print(f'task number is: {task_number}')

    for i in range(20):
        task_p = PythonOperator(
            task_id='task_number_' + str(i),
            python_callable=print_task_number,
            op_kwargs={'task_number': i},
        )
        task_b >> task_p
    