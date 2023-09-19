"""
hw_n-knjazeva-24_5
"""
from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import timedelta, datetime
from textwrap import dedent

with DAG(
        'hw_n-knjazeva-24_5',
        default_args={
            'depends_on_past': False,
            'email': ['airflow@example.com'],
            'email_on_failure': False,
            'email_on_retry': False,
            'retries': 1,
            'retry_delay': timedelta(minutes=5),  # timedelta из пакета datetime
        },
        start_date=datetime(2023, 9, 18),
        schedule_interval=timedelta(days=1)
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
        task_id='temp_task',
        bash_command=templated_command
    )