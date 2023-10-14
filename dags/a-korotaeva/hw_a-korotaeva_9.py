from datetime import datetime, timedelta
from textwrap import dedent
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

def xc_push(ti):
    ti.xcom_push(
        key='sample_xcom_key',
        value='xcom test'
    )

def xc_pull(ti):
    a = ti.xcom_pull(
        key='sample_xcom_key', task_ids='push_xcom'
    )
    print(a)


with DAG(
    'hw_9_a-korotaeva',
    default_args={
        'depends_on_past': False,
        'email': ['airflow@example.com'],
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 1,
        'retry_delay': timedelta(minutes=5),
    },
    description='A simple tutorial DAG', schedule_interval=timedelta(days=1),
    start_date=datetime(2022, 10, 13),
    catchup=False
) as dag:

    t1 = PythonOperator(task_id='push_xcom', python_callable=xc_push)

    t2 = PythonOperator(task_id='pull_xcom', python_callable=xc_pull)

    t1 >> t2

