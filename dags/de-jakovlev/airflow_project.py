from datetime import timedelta, datetime
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from textwrap import dedent
from airflow.models import Variable
from airflow.hooks.base_hook import BaseHook

from airflow import DAG


default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}

with DAG(
    'hw_10_de-jakovlev',
    start_date=datetime(2021, 1, 1),
    max_active_runs=2,
    schedule_interval=timedelta(minutes=30),
    default_args=default_args,
    catchup=False
) as dag:
    
    def push_func(ti):
        return "Airflow tracks everything"


    def pull_func(ti):
        info = ti.xcom_pull(key='return_value', task_ids='x_com_push')
        print(info)

    p1 = PythonOperator(
        task_id='x_com_push',
        python_callable=push_func,
    )

    p2 = PythonOperator(
        task_id='x_com_pull',
        python_callable=pull_func,
    )
    p1 >> p2








