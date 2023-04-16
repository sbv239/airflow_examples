from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator


def push_hidden():
    return "Airflow tracks everything"


def pull_hidden(ti):
    ti.xcom_pull(
        key="return_value",
        task_ids="push_hidden_xm"
    )


with DAG(
    'aakulzhanov_task_10',
    default_args={
        'depends_on_past': False,
        'email': ['airflow@example.com'],
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 1,
        'retry_delay': timedelta(minutes=5),  # timedelta из пакета datetime
   },
    description='A simple Task 10',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2023, 1, 1),
    catchup=False,
    tags=['example'],
) as dag:
    task_1 = PythonOperator(
        task_id='push_hidden_xm',
        python_callable=push_hidden
    )
    task_2 = PythonOperator(
        task_id="pull_hidden_xm",
        python_callable=pull_hidden
    )

    task_1 >> task_2