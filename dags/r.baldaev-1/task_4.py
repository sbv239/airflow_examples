"""
Test documentation
"""
from datetime import datetime, timedelta
from textwrap import dedent

from airflow import DAG

from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator


def print_task_number(task_number):
    print(f"task number is: {task_number}")


with DAG(
    'task_4_r_baldaev',
    default_args={
        'depends_on_past': False,
        'email': ['airflow@example.com'],
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 1,
        'retry_delay': timedelta(minutes=5),  # timedelta из пакета datetime
    },
    description='Task 4 - print ts and run_id',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2022, 1, 1),
    catchup=False,
    tags=['r.baldaev-1'],
) as dag:
    templated_command = dedent(
        """
            {% for i in range(5) %}
                echo "{{ ts }}"
                echo "{{ run_id }}"
            {% endfor %}
        """
    )

    t = BashOperator(
        task_id='templated',
        depends_on_past=False,
        bash_command=templated_command,
    )
