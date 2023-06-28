"""
Test documentation
"""
from datetime import datetime, timedelta
from textwrap import dedent

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

with DAG(
    'hw_p-bochkarev_3',

    default_args={
        'depends_on_past': False,
        'email': ['airflow@example.com'],
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 1,
        'retry_delay': timedelta(minutes=5),
    },
    description='Task 3',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2022, 1, 1),
    catchup=False,
    tags=['p-bochkarev'],
) as dag:

    for i in range(10):
        t1 = BashOperator(
            task_id='echo_' + str(i),
            bash_command=f'echo {i}',
        )
        t1.doc_md = dedent(
        """
        #### DOCUMENTATION
        'code', *italic*, **bold**
        """
        )
    def print_task_number(task_number):
        print(f'task number is: {task_number}')

    for i in range(20):
        t2 = PythonOperator(
            task_id='task_number_' + str(i),
            python_callable=print_task_number,
            op_kwargs={'task_number': i},
        )
        t2.doc_md = dedent(
        """
        #### DOCUMENTATION
        'code', *italic*, **bold**
        """
        )
t1 >> t2
