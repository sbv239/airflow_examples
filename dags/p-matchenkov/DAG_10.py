import datetime

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from textwrap import dedent

from datetime import timedelta


def send_xcom(ti):
    return "Airflow tracks everything"


def receive_xcom(ti):
    value = ti.xcom_pull(
        key='return_value',
        task_ids='xcom_sender' # передать task_id из  sender_task (38 срока)
    )
    print(value)

with DAG(
    'hw_p-matchenkov_10',
    default_args={
            'depends_on_past': False,
            'email': ['airflow@example.com'],
            'email_on_failure': False,
            'email_on_retry': False,
            'retries': 1,
            'retry_delay': timedelta(minutes=5),
    },
    description='task 10 dag',
    start_date=datetime.datetime(2023, 10, 16),
    catchup=False,
    tags=['matchenkov']
) as dag:

    sender_task = PythonOperator(
        task_id="xcom_sender",
        python_callable=send_xcom
    )

    receiver_task = PythonOperator(
        task_id="xcom_receiver",
        python_callable=receive_xcom
    )

    sender_task >> receiver_task