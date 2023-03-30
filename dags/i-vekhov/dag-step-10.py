from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta


def print_message():
    message = 'Airflow tracks everything'
    print(message)
    return message


def get_message(ti):
    message = ti.xcom_pull(
        key='return_value',
        task_ids='printing_blank_message'
    )
    return message


with DAG(
    'hw_7_i-vekhov',
    default_args={
        'depends_on_past': False,
        'email': ['airflow@example.com'],
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 1,
        'retry_delay': timedelta(minutes=5),  # timedelta из пакета datetime
    },
    description='hw_7_i-vekhov',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2021, 3, 29),
    catchup=False,
    tags=['hw_7_i-vekhov'],
) as dag:
    print_airflow_message = PythonOperator(
        task_id='printing_blank_message',
        python_callable=print_message
    )
    get_xcom_message = PythonOperator(
        task_id='pull_message',
        python_callable=get_message
    )
    print_airflow_message >> get_xcom_message
