from datetime import datetime, timedelta
from textwrap import dedent

from airflow import DAG

from airflow.operators.bash import BashOperator
from airflow.operators.python_operator import PythonOperator


def push_value(ti):
    ti.xcom_push(
        key="sample_xcom_key",
        value="xcom test"
    )


def pull_value(ti):
    result = ti.xcom_pull(
        key="sample_xcom_key",
        task_ids="write_to_xcom"
    )
    print(result)


with DAG(
        'hw_n-shishkin_9',
        default_args={
            'depends_on_past': False,
            'email': ['airflow@example.com'],
            'email_on_failure': False,
            'email_on_retry': False,
            'retries': 1,
            'retry_delay': timedelta(minutes=5),  # timedelta из пакета datetime
        },
        schedule_interval=timedelta(days=1),
        start_date=datetime(2022, 1, 1),
        catchup=False,
        tags=['hw_n-shishkin_9'],
) as dag:
    t1 = PythonOperator(
        task_id="write_to_xcom",
        python_callable=push_value
    )
    t2 = PythonOperator(
        task_id="take_from_xcom",
        python_callable=pull_value
    )
    t1 >> t2
