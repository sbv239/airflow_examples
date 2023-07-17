"""
--DAG docs will be there--
"""
from datetime import datetime, timedelta
from textwrap import dedent
from airflow import DAG

from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator


def first_python_operator(*args, **kwargs):
    # from loguru import logger
    # logger.info(args)
    # logger.info(kwargs)
    # print(kwargs['ds'])
    print(args)
    print(kwargs)


default_args={
    'depends_on_past': False,
    'owner': 'and-f',
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(dag_id='hw_2_and-f',
         default_args=default_args,
         description='--DAG description here--',
         schedule_interval=timedelta(days=1),
         start_date=datetime(2023, 7, 17),
         catchup=False,
         tags=['--DAG tag here--']) as dag:

    dag.doc_md = __doc__
    t1 = BashOperator(task_id='print_pwd', bash_command='pwd')
    t1.doc_md = dedent("""#### Task Documentation in markdown for 'print_pwd' task""")
    t2 = PythonOperator(task_id='first_python_operator', python_callable=first_python_operator)
    t2.doc_md = dedent("""#### Task Documentation in markdown for 'first_python_operator' task""")

    t1 >> t2
