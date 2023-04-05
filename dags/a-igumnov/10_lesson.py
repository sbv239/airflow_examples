from datetime import datetime, timedelta
from textwrap import dedent

from airflow import DAG

from airflow.operators.python import PythonOperator

def text_return():
    return "Airflow tracks everything"

def get_value(ti):
    result = ti.xcom_pull(
        key='return_value',
        task_ids='push'
        )
    print(result)

with DAG(
    'a-igumnov_task_10',

    default_args={
    'depends_on_past': False,
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),  # timedelta из пакета datetime
    },
    description = 'hw_10_a-igumnov',
    schedule_interval = timedelta(days=1),
    start_date=datetime(2023, 1, 1),
    catchup=False,
    tags=['hw_10_a-igumnov_XCom']


) as dag:

    push_task = PythonOperator(
        task_id='push',
        python_callable=text_return
    )
    pull_task = PythonOperator(
        task_id='pull',
        python_callable=get_value
    )
    
    push_task >> pull_task