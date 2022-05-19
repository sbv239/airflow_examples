"""
Start-ml Airflow Task 8
"""
from datetime import datetime, timedelta
from textwrap import dedent

from airflow import DAG

from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

def xcom_push(ti):
    ti.xcom_push(
        key='sample_xcom_key',
        value="xcom test"
    )

def xcom_pull(ti):
    xcom_test = ti.xcom_pull(
        key='sample_xcom_key',
        task_ids='push_xcom'
    )
    print(xcom_test)

default_args={
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    'hw_8_o-zamoschin',
    default_args=default_args,
    schedule_interval=timedelta(days=1),
    start_date=datetime(2022, 1, 1),
    catchup=False,
) as dag:
    
    t1 = PythonOperator(
        task_id = 'push_xcom',
        python_callable=xcom_push,
    )
    t2 = PythonOperator(
        task_id = 'pull_xcom',
        python_callable=xcom_pull,
    )

    t1 >> t2