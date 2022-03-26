from airflow import DAG

from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

from datetime import datetime, timedelta
from textwrap import dedent

with DAG(
    'hw_9_j-gladkov-6',
    default_args={
        'depends_on_past': False,
        'email': ['airflow@example.com'],
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 1,
        'retry_delay': timedelta(minutes=5),
    },
    description = "implicit xcom", # name
    schedule_interval = timedelta(days=1),
    start_date = datetime(2022, 3, 22),
    catchup = False,
    tags = ['hw_9'],
) as dag:

    def returner():
        return "Airflow tracks everything"

    def receiver(ti):
        message = ti.xcom_pull(
        key = "return_value",
        task_ids = 'pusher'

    t1 = PythonOperator(
        task_id = 'pusher',
        python_callable = returner,
    )

    t1 = PythonOperator(
        task_id = 'puller',
        python_callable = receiver,
    )

    t1 >> t2
