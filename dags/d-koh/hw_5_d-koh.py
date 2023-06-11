from datetime import datetime, timedelta
from textwrap import dedent

from airflow import DAG
from airflow.operators.bash import BashOperator

with DAG(
        'HW_5_d-koh',
        default_args={
            'depends_on_past': False,
            'email': ['airflow@example.com'],
            'email_on_failure': False,
            'email_on_retry': False,
            'retries': 1,
            'retry_delay': timedelta(minutes=5),
        },
        description='DAG from step 5',
        schedule_interval=timedelta(days=1),
        start_date=datetime(2022, 2, 10),
        catchup=False,
        tags=['kokh'],
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
        task_id='Template_command',
        bash_command=templated_command
    )
