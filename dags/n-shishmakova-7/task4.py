from datetime import datetime, timedelta
from textwrap import dedent

from airflow import DAG

from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

with DAG(
    'hw_4_n-shishmakova-7',
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
    description='DAG for 4 task in lesson 11',
    # Как часто запускать DAG
    schedule_interval=timedelta(days=1),
    start_date=datetime(2022, 5, 1),
    # Запустить за старые даты относительно сегодня
    catchup=False,
    # теги, способ помечать даги
    tags=['hw4'],
) as dag:
    templated_command = dedent(
        """
    {% for i in range(5) %}
        echo "{{ ts }}"
        echo "{{ run_id}}"
    {% endfor %}
    """
    )
    t1 = BashOperator(
        task_id='templated',
        bash_command=templated_command,
    )