from airflow import DAG
from datetime import timedelta, datetime
from textwrap import dedent

from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

with DAG(
    'rakhimova_task4',
    # Параметры по умолчанию для тасок
    default_args={
        'depends_on_past': False,
        'email': ['airflow@example.com'],
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 1,
        'retry_delay': timedelta(minutes=5),  # timedelta из пакета datetime

    },
    description='DAG4 Rakhimova',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2022, 3, 25),
    catchup=False,
    tags=['hehe'],
) as dag:

    templated_command = dedent(
        """
        {% for i in range(5) %}
            echo "{{ ts }}"
            echo "{{ run_id }}"
        {% endfor %}
        """
    )

    t1 = BashOperator(
            task_id='templated',
            bash_command=templated_command,
    )

