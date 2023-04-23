from datetime import datetime, timedelta
from textwrap import dedent

from airflow import DAG

from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator


def first_handler(ti):
    ti.xcom_push(
        key='sample_xcom_key',
        value='xcom test'
    )

def second_handler(ti):
    value = ti.xcom_pull(key='sample_xcom_key', task_ids='first')
    print(value)

with DAG(
        'e_9_demets',
        # Параметры по умолчанию для тасок
        default_args={
            'depends_on_past': False,
            'email': ['airflow@example.com'],
            'email_on_failure': False,
            'email_on_retry': False,
            'retries': 1,
            'retry_delay': timedelta(minutes=5),  # timedelta из пакета datetime
        },
        description='A simple tutorial DAG',
        schedule_interval=timedelta(days=1),
        start_date=datetime(2022, 1, 1),
        catchup=False,
        tags=['demets'],
) as dag:
    t1 = PythonOperator(
        task_id='first',
        python_callable=first_handler,
    )

    t2 = PythonOperator(
        task_id='second',
        python_callable=second_handler,
    )

    t1 >> t2
