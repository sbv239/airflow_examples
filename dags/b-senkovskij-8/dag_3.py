"""
First dag
"""
from datetime import datetime, timedelta
from textwrap import dedent

from airflow import DAG

from airflow.operators.bash import BashOperator
from airflow.operators.python_operator import PythonOperator

with DAG(
    'senkovskiy_dag2',

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
    tags=['senkovskiy_dag2'],

) as dag:

    for i in range(10):

        task1 = BashOperator(
            task_id=f'print_echo_{i}',  
            bash_command=f"echo {i}",  
        )

        task1.doc_md = dedent(
        """\
            #### **task 1 Documentation**
            This taks is running the *bash command*: `echo {i}`,
        """) 

    def print_task_number(**kwargs):
        task_number = kwargs['task_number']
        print(f'task number is: {task_number}')

    for i in range(20):
        
        task2 = PythonOperator(
            task_id=f'print_state_{i}', 
            python_callable=print_task_number,
            op_kwargs={'task_number': i}
        )

        task2.doc_md = dedent(
        """\
            #### **task 2 Documentation**
            This taks is running the *python code*: `print(task number is: {i})`,
        """) 

    task1 >> task2

