"""
Start-ml Airflow Task 2
"""
from datetime import datetime, timedelta
from textwrap import dedent

from airflow import DAG

from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

def print_task(task_number):
    print(f"task number is: {task_number}")

default_args={
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    'hw_2_o-zamoschin',
    default_args=default_args,
    schedule_interval=timedelta(days=1),
    start_date=datetime(2022, 1, 1),
    catchup=False,
) as dag:
    
    for i in range(10):
        t1 = BashOperator(
            task_id='echo_' + str(i),
            bash_command=f'echo {i}',
        )
        
        t1.doc_md = dedent(
            """
            # Task Documentation
            This **task** prints *bash* command `echo` and the task number 
            """
        )

    for i in range(20):
        t2 = PythonOperator(
            task_id='print_task_' + str(i),
            python_callable=print_task,
            op_kwargs={'task_number': i}
        )

        t2.doc_md = dedent(
            """
            # Task Documentation
            This **task** uses *python* function `print_task` which prints the task number 
            """
        )
    
    t1 >> t2