from airflow import DAG

from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

from datetime import datetime, timedelta
from textwrap import dedent

with DAG(
    'hw_8_j-gladkov-6',
    default_args={
        'depends_on_past': False,
        'email': ['airflow@example.com'],
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 1,
        'retry_delay': timedelta(minutes=5),
    },
    description = "More Args",
    schedule_interval = timedelta(days=1),
    start_date = datetime(2022, 3, 22),
    catchup = False,
    tags = ['hw_8'],
) as dag:

    def sender(ti):
        ti.xcom_push(
        key = "sample_xcom_key",
        value = "xcom test"
        )

    def receiver(ti):
        testing_increases = ti.xcom_pull(
        key = "sample_xcom_key",
        task_ids = 'pusher'
        )
        print("sample_xcom_key")

    t1 = PythonOperator(
        task_id = 'pusher',
        python_callable = sender,
    )

    t2 = PythonOperator(
        task_id = 'puller',
        python_callable = receiver,

    )

    t1 >> t2
