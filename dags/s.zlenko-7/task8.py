"""
Task 8: https://lab.karpov.courses/learning/84/module/1049/lesson/10040/29383/139293/
"""
from datetime import datetime, timedelta
from textwrap import dedent

from airflow import DAG

from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

default_args={
    'depends_on_past': False,
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    }

with DAG(
    'hw_8_s.zlenko-7',
    default_args = default_args,
    schedule_interval=timedelta(days=1),
    start_date=datetime(2022, 1, 1),
    catchup=False,
) as dag:

        def send_data(ti):
            ti.xcom_push(
                key='sample_xcom_key',
                value='xcom test'
            )

        def get_data(ti):
            xcom_test = ti.xcom_pull(
                key='sample_xcom_key',
                task_ids='send_data'
            )
            print(xcom_test)

        t1 = PythonOperator(
            task_id='send_data',
            python_callable=send_data
        )
        t2 = PythonOperator(
            task_id='get_data',
            python_callable=get_data
        )

        t1 >> t2