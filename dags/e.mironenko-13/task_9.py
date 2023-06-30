from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python_operator import PythonOperator
from textwrap import dedent

from datetime import timedelta, datetime

def push_task(ti):
    ti.xcom_push(
        key = "sample_xcom_key",
        value = "xcom test"
    )
def pull_task(ti):
    pr = ti.xcom_pull(
        key = "sample_xcom_key",
        task_ids = 'push_task'
    )
    print(pr)
with DAG(
    'hw_e.mironenko-13_9',
    default_args={
        'depends_on_past': False,
        'email': ['airflow@example.com'],
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 1,
        'retry_delay': timedelta(minutes=5),  # timedelta из пакета datetime
            },
    start_date=datetime(2023, 6, 26),
    schedule_interval=timedelta(days=1),
    catchup=False,
    tags = ['e.mironenko-13']
) as dag:
    t1 = PythonOperator(
        task_id='push_task',
        python_callable=push_task
    )
    t2 = PythonOperator(
        task_id='pull_task',
        python_callable=pull_task
    )

t1 >> t2