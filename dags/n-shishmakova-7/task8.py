from datetime import datetime, timedelta
from textwrap import dedent

from airflow import DAG

from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator


def push_data(ti):
    ti.xcom_push(
        key="sample_xcom_key",
        value="xcom test"
    )


def pull_data(ti):
    to_print = ti.xcom_pull(
        key="sample_xcom_key",
        task_ids="push"
    )
    print(to_print)


with DAG(
        'hw_8_n-shishmakova-7',
        # Параметры по умолчанию для тасок
        default_args={
            'depends_on_past': False,
            'email': ['airflow@example.com'],
            'email_on_failure': False,
            'email_on_retry': False,
            'retries': 1,
            'retry_delay': timedelta(minutes=5),  # timedelta из пакета datetime
        },
        # Описание DAG (не тасок, а самого DAG)
        description='DAG for 8 task in lesson 11',
        # Как часто запускать DAG
        schedule_interval=timedelta(days=1),
        start_date=datetime(2022, 5, 1),
        # Запустить за старые даты относительно сегодня
        catchup=False,
        # теги, способ помечать даги
        tags=['hw8'],
) as dag:
    t1 = PythonOperator(
        task_id='push',  # нужен task_id, как и всем операторам
        python_callable=push_data
    )
    t2 = PythonOperator(
        task_id='pull',  # нужен task_id, как и всем операторам
        python_callable=pull_data
    )

    t1 >> t2
