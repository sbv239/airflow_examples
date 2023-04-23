from datetime import datetime, timedelta
from textwrap import dedent

from airflow import DAG

from airflow.operators.bash import BashOperator

with DAG(
    'hw_5_mi-ponomarev',
    default_args={
        'depends_on_past': False,
        'email': ['airflow@example.com'],
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 1,
        'retry_delay': timedelta(minutes=5),
    },
        description='task_5',
        schedule_interval=timedelta(days=365),
        start_date=datetime(2023, 4, 22),
        catchup=False,
        tags=['task_5']
) as dag:
        timestamp = dedent(
                """
                {% for i in range(5) %}
                    echo "{{ ts }}"
                    echo "{{ run_id }}"
                {% endfor %}
                """
        )
        t1 = BashOperator(
                task_id = 'print_timestamp',
                bash_command=timestamp,
        )
