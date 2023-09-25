from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta
from textwrap import dedent

default_args = {
    'depends_on_past': False,
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
        'hw_4_d-korjakov',
        default_args=default_args,
        start_date=datetime(2023, 9, 24),
        schedule_interval=timedelta(days=1),
) as dag:
    templated_command = dedent(
        """
    {% for i in range(5) %}
        echo "{{ ts }}"
        echo "{{ run_id }}"
    {% endfor %}
    """
    )

    for i in range(5):
        BashOperator(
            task_id=f'task_{i}',
            bash_command=templated_command,
        )
