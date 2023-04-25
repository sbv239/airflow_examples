"""
step_5 DAG
"""
from datetime import datetime, timedelta
from textwrap import dedent

from airflow import DAG

from airflow.operators.bash import BashOperator


with DAG(
    'hw_r-shahvaly_5',

    default_args={
        'depends_on_past': False,
        'email': ['airflow@example.com'],
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 1,
        'retry_delay': timedelta(minutes=5),
    },
    description='DAG for step_5',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2023, 1, 1),
    catchup=False,
    tags=['r-shahvaly'],
) as dag:

    def print_task_number(task_number, **kwargs):
        print(task_number)

    command = dedent("""
        {% for i in range(5) %}
            echo "{{ ts }}"
            echo "{{ run_id }}"
        {% endfor %}
    """)
    task = BashOperator(
        task_id='bash_echo_0',
        bash_command=command,
    )