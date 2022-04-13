"""
Данный DAG выполняет в консоли комманду "pwd" и выводит аргумент "ds
"""

from datetime import datetime, timedelta

from airflow import DAG

from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator


#PythonOperator
def print_context(ds):
    print(ds)


#DAG
with  DAG(
    'hw_1_j-gladkov-6',
    default_args={
        'depends_on_past': False,
        'email': ['airflow@example.com'],
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 1,
        'retry_delay': timedelta(minutes=5),  # timedelta из пакета datetime
    },
    description = 'HW-step-1',
    schedule_interval = timedelta(days=1),
    start_date = datetime(2022, 1, 21),
    catchup = False,
    tags = ['hw_1'],
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