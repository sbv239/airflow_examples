from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta

def get_value(ti):
    ti.xcom_push(
        key='sample_xcom_key',
        value='xcom test'
    )

def print_value(ti):
    ti.xcom_pull(
        task_ids='get_value', 
        key='sample_xcom_key'
    )

with DAG(
    'xcom_dag08',
    start_date=datetime(2022, 5, 7),
    max_active_runs=2,
    schedule_interval=timedelta(minutes=30),
    catchup=False
) as dag:
    get_value = PythonOperator(
        task_id = 'get_value',
        python_callable=get_value,
    )
    print_value = PythonOperator(
        task_id = 'print_value',
        python_callable=print_value,
    )

    get_value >> print_value

