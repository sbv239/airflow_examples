"""
--DAG docs will be there--
"""
from datetime import datetime, timedelta
from textwrap import dedent
from airflow import DAG

from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator


def first_python_operator(ti, *args, **kwargs):
    # ti.xcom_push(key='sample_xcom_key', value="xcom test")
    return "Airflow tracks everything"


def second_python_operator(ti, *args, **kwargs):
    # val = ti.xcom_pull(key='sample_xcom_key', task_ids='t1')
    val = ti.xcom_pull(key='return_value', task_ids='t1')
    print(val)


default_args={
    'depends_on_past': False,
    'owner': 'and-f',
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(dag_id='hw_10_and-f',
         default_args=default_args,
         description='--DAG description here--',
         schedule_interval=timedelta(days=1),
         start_date=datetime(2023, 7, 17),
         catchup=False,
         tags=['--DAG tag here--']) as dag:

    dag.doc_md = __doc__
    t1 = PythonOperator(task_id='t1', python_callable=first_python_operator)
    t2 = PythonOperator(task_id='t2', python_callable=second_python_operator)

    t1 >> t2