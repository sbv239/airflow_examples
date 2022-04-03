from datetime import datetime, timedelta
from textwrap import dedent
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

def push_operator(ti):
        ti.xcom_push(
            key = 'sample_xcom_key',
            value = 'xcom test'
        )
    
def pull_operator(ti):
    sample_xcom_key = ti.xcom_pull(
        key = 'sample_xcom_key',
        task_ids = 'pull_data'
    )
    
    print(f'sample_xcom_key {sample_xcom_key}')

with DAG(
    'hw_8_s-hodzhabekova-6',
    default_args={
        'depends_on_past': False,
        'email': ['airflow@example.com'],
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 1,
        'retry_delay': timedelta(minutes=5),
    },
    description='A simple tutorial DAG',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2022, 1, 1),
    catchup=False,
    tags=['hw_8', 'khodjabekova'],
) as dag:

    t1 = PythonOperator(
        task_id='push_data',
        python_callable=push_operator,
    )

    t2 = PythonOperator(
        task_id='pull_data',
        python_callable=pull_operator,

    )

    t1 >> t2