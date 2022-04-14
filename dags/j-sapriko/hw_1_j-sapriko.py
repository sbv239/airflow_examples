"""
Данный DAG выполняет в консоли комманду "pwd" и выводит аргумент "ds
"""

from datetime import datetime, timedelta
from textwrap import dedent

from airflow import DAG

from airflow.operators.bash import BashOperator
from airflow.operators.python_operator import PythonOperator


#PythonOperator
def print_context(ds, **kwargs):
    print(ds)


#DAG
with  DAG(
default_args={
    'depends_on_past': False,
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),  # timedelta из пакета datetime
},
) as dag:
    t1 = BashOperator(
        task_id = 'run PWD command',
        bash_command = 'pwd',
    )

    t2 = PythonOperator(
        task_id = 'print_ds',
        python_callable = print_context,

    )
    dag.doc_md = __doc__

#Порядок выполнения команд
    t1 >> t2