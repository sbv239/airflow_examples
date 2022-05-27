"""
## **Assignment 3 DAG documentation**
### ***Task 1***
Call 10 echo *bash* commands using `for`-loop.
### ***Task 2***
Print 20 task numbers like `task_number is: {task_number}` by using `for`-loop
"""
from datetime import datetime, timedelta
from textwrap import dedent

from airflow import DAG

from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

with DAG(
        'gul_assignment_3',
        default_args={
            'depends_on_past': False,
            'email': ['airflow@example.com'],
            'email_on_failure': False,
            'email_on_retry': False,
            'retries': 1,
            'retry_delay': timedelta(minutes=5)
        },
        description='This DAG calls 10 echo bash commands and 20 python tasks using for-loops',
        schedule_interval=timedelta(days=1),
        start_date=datetime(2022, 5, 26),
        catchup=False,
        tags=['gul_dag_3']
) as dag:
    for i in range(10):
        b_task = BashOperator(
            task_id='bash_task_' + str(i),
            bash_command=f'echo {i}'
        )


    def print_task_number(task_number):
        print(f'task number is: {task_number}')
        return "If you see this the task was successful"


    for j in range(20):
        p_task = PythonOperator(
            task_id='python_task_' + str(j),
            python_callable=print_task_number,
            op_kwargs={'task_number': j}
        )
    dag.doc_md = __doc__
    b_task >> p_task
