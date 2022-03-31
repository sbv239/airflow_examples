"""
Second Airflow trial

"""
from datetime import datetime, timedelta
from textwrap import dedent

from airflow import DAG

from airflow.operators.bash import BashOperator
from airflow.operators.python_operator import PythonOperator

def print_task(task_number):
    print(f"task number is: {task_number}")
    

with DAG(
's-filonov-6_hw3',
default_args={
    'depends_on_past': False,
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),  # timedelta из пакета datetime
},

description='A simple tutorial DAG',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2022, 3, 28),
    catchup=False,
    tags=['learning'],
) as dag:
    for i in range(10):
        t1 = BashOperator(
            task_id='loop_'+ str(i), 
            bash_command=f'echo {i}',  
    )

    t1.doc_md = dedent(
        """\
    #BashOperator tasks:
    Via loop creating  *10 tasks*, each **printing** a number!
    Current print of number {i} implemented by bash-command `echo {i}`
    """
    )
    for k in range(10,30):
        t2 = PythonOperator(
        task_id='python_' + str(k),
        python_callable = print_task,
        op_kwargs={'task_number': k},
    )

    t2.doc_md = dedent(
        """\
    #PythonnOperator tasks:
    Via loop creating  *20 tasks*, each **printing** a number!
    Current print of number {k} implemented with python function `print_task`
    """)