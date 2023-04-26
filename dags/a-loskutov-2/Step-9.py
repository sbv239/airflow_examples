"""
Test documentation
"""
from datetime import datetime, timedelta
from textwrap import dedent

from airflow import DAG

from airflow.operators.python_operator import PythonOperator
from airflow.operators.bash import BashOperator

with DAG(
    'hw_9_a_loskutov',
    # Параметры по умолчанию для тасок

    default_args={
        'depends_on_past': False,
        'email': ['airflow@example.com'],
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 1,
        'retry_delay': timedelta(minutes=5),  # timedelta из пакета datetime
    },

    description='My first DAG',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2023, 4, 25),
    catchup=False,
    tags=['Loskutov_hm'],
) as dag:
    

    def push_in_xcom(ti):
            ti.xcom_push(
                    key="sample_xcom_key",
                    value="xcom test"
            )

    def pull_in_xcom(ti):
            result=ti.xcom_pull(
                    key="sample_xcom_key",
                    task_ids='push_xcom'
            )
            return result

    push_xcom = PythonOperator(
            task_id='push_xcom',
            python_callable=push_in_xcom,
    )

    pull_xcom =  PythonOperator(
            task_id='pull_xcom',
            python_callable=pull_in_xcom,
    )

    push_xcom >> pull_xcom

