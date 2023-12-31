from datetime import datetime, timedelta
from airflow import DAG
from textwrap import dedent

from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

with DAG(
    'hw_a-ruzhich_5',
    default_args={
        'depends_on_past': False,
        'email': ['airflow@example.com'],
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 1,
        'retry_delay': timedelta(minutes=5),
    },
    description='lesson 11 ex 5 DAG',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2023, 9, 29),
) as dag:
    templated_command = dedent(
        """
        {% for i in range(5) %}
            echo "{{ ts }}"
            echo "{{ run_id }}"
        {% endfor %}
        """
    )
    task1 = BashOperator(
        task_id='templated_command_5',
        depends_on_past=False,
        bash_command=templated_command
    )
    task1