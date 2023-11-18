from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator

def push_value(ti):
    value = ti.xcom_push(
        key='sample_xcom_key',
        value='xcom_test'
    )
    print(value)

def print_value(ti):
    print(ti.xcom_pull(
        key='sample_xcom_key',
        task_ids='push_value'
    ))

with DAG(
    'hw_a-dolganov_9',
    default_args={
        'depends_on_past': False,
        'email': ['airflow@example.com'],
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 1,
        'retry_delay': timedelta(minutes=5),
    },
    description='HW 9 a-dolganov',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2023, 1, 1),
    catchup=False,
    tags=['a-dolganov'],
) as dag:

    put_data = PythonOperator(
            task_id ='push_value',
            python_callable = push_value,
    )
    
    get_data = PythonOperator(
            task_id ='print_value',
            python_callable = print_value,
    )

    put_data >> get_data