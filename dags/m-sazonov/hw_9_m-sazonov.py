from datetime import datetime, timedelta

from airflow import DAG

from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from textwrap import dedent

with DAG(
    'hw_9_m-sazonov',
    default_args={
    'depends_on_past': False,
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    },
    description='hw_9_m-sazonov',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2023, 6, 27),
    catchup=False,
) as dag:

    def push_xcom(ti):
            ti.xcom_push(
                key='sample_xcom_key',
                value='xcom test'
            )
    def pull_xcom(ti):
            task = ti.xcom_pull(
                key='sample_xcom_key',
                task_ids='xcom_pull'
            )
            print(task)

    t1 = PythonOperator(
            task_id='xcom_pull',
            python_callable=push_xcom)
    t2 = PythonOperator(
            task_id='result',
            python_callable=pull_xcom,
        )
    t1 >> t2
