"""
First Airflow trial

"""
from datetime import datetime, timedelta
from textwrap import dedent

from airflow import DAG

from airflow.operators.bash import BashOperator
from airflow.operators.python_operator import PythonOperator

def print_context(ds):
    print(ds)
    return('Printed even this!!!')

with DAG(
's-filonov-6_hw1',
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

    t1 = BashOperator(
        task_id='bash_pwd', 
        bash_command='pwd',  
    )

    t1.doc_md = dedent(
        """\
    Printing pwd, bash.

    """
    )

    t2 = PythonOperator(
        task_id='py_printds',
        python_callable = print_context,
    )

    t1 >> t2