import datetime

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from textwrap import dedent

from datetime import timedelta


def send_xcom(ti):
    ti.xcom_push(
        key="sample_xcom_key",
        value="xcom test"
    )


def receive_xcom(ti):
    value = ti.xcom_pull(
        key="sample_xcom_key",
        task_ids="xcom_test_sender_task"
    )
    print(value)


with DAG(
        'hw_j-rzayev_9',
        default_args={
                'depends_on_past': False,
                'email': ['airflow@example.com'],
                'email_on_failure': False,
                'email_on_retry': False,
                'retries': 1,
                'retry_delay': timedelta(minutes=5),
        },
        description='task_9',
        start_date=datetime.datetime(2023, 10, 20),
        catchup=False,
        tags=['task_9', 'lesson_11', 'j-rzayev']
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
