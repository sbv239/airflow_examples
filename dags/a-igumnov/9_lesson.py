from datetime import datetime, timedelta
from textwrap import dedent

from airflow import DAG

from airflow.operators.python import PythonOperator

def push_data(ti):
    ti.xcom_push(
        key = 'sample_xcom_key',
        value = 'xcom test'
    )

def pull_data(ti):
    output = ti.xcom_pull(
        key = 'sample_xcom_key',
        task_ids = 'push'
    )
    print(output)

with DAG(
    'a-igumnov_task_9',

    default_args={
    'depends_on_past': False,
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),  # timedelta из пакета datetime
    },
    description = 'hw_9_a-igumnov',
    schedule_interval = timedelta(days=1),
    start_date=datetime(2023, 1, 1),
    catchup=False,
    tags=['hw_9_a-igumnov_XCom']


) as dag:

    push_task = PythonOperator(
        task_id='push',
        python_callable=push_data
    )
    pull_task = PythonOperator(
        task_id='pull',
        python_callable=pull_data
    )
    
    push_task >> pull_task