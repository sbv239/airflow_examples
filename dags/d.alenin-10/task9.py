from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from textwrap import dedent

default_args = {
        'depends_on_past': False,
        'email': ['airflow@example.com'],
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 1,
        'retry_delay': timedelta(minutes=5)
    }


def push_xcom(ti):
    ti.xcom_push(
        key="sample_xcom_key",
        value="xcom test"
    )


def get_xcom(ti):
    data = ti.xcom_pull(
        key="sample_xcom_key",
        task_ids='get_data'
    )
    print(data)


with DAG(
    'hw_9_d.alenin-10',
    default_args=default_args,
    description='Simple first dag',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2022, 3, 20),
    catchup=False
) as dag:

    push_task = PythonOperator(
        task_id="get_data",
        python_callable=push_xcom
    )

    get_task = PythonOperator(
        task_id="analyze",
        python_callable=get_xcom
    )

    push_task >> get_task
