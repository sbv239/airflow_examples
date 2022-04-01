from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

from textwrap import dedent

with DAG(
    'task_4',
    default_args={
        'depends_on_past': False,
        'email': ['airflow@example.com'],
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 1,
        'retry_delay': timedelta(minutes=5)
    },
    description='DAG for task 4',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2022, 3, 25),
    catchup=False,
    tags=['DP HW4']
) as dag:
        templated_command = dedent(
                """
                {% for i in range(5) %}
                    echo "{{ ts }}"
                {% endfor %}
                echo "{{ run_id }}"
                """
        )

        task1 = BashOperator(
                task_id='template_command_print',
                bash_command=templated_command,
        )

        task1
