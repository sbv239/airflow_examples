"""
--DAG docs will be there--
"""
from datetime import datetime, timedelta
from textwrap import dedent
from airflow import DAG

from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator


def first_python_operator(task_number, *args, **kwargs):
    # from loguru import logger
    # logger.info(args)
    # logger.info(kwargs)
    # print(kwargs['ds'])
    print(f'task number is: {task_number}')



default_args={
    'depends_on_past': False,
    'owner': 'and-f',
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(dag_id='hw_3_and-f',
         default_args=default_args,
         description='--DAG description here--',
         schedule_interval=timedelta(days=1),
         start_date=datetime(2023, 7, 17),
         catchup=False,
         tags=['--DAG tag here--']) as dag:

    dag.doc_md = __doc__
    for i in range(1,31):
        if i<=10:
            b_task = BashOperator(task_id=f'b_task_{i}',
                                  bash_command=f'echo {i}')
        else:
            p_task = PythonOperator(task_id=f'p_task_{i}',
                                    python_callable=first_python_operator,
                                    op_kwargs={'task_number': i})
