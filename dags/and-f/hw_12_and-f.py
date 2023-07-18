"""
--DAG docs will be there--
"""
from datetime import datetime, timedelta
from textwrap import dedent
from airflow import DAG

from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator


def first_python_operator(*args, **kwargs):
    from airflow.models import Variable
    is_startml = Variable.get("is_startml")
    is_startml2 = kwargs['var']
    print(f'Это через Variable.get("is_startml"): {is_startml}')
    print(f'Это через kwargs["var"]: {is_startml2}')


default_args={
    'depends_on_past': False,
    'owner': 'and-f',
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(dag_id='hw_12_and-f',
         default_args=default_args,
         description='--DAG description here--',
         schedule_interval=timedelta(days=1),
         start_date=datetime(2023, 7, 17),
         catchup=False,
         tags=['--DAG tag here--']) as dag:

    dag.doc_md = __doc__
    t1 = PythonOperator(task_id='t1', python_callable=first_python_operator)
