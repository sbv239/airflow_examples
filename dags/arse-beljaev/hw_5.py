from datetime import datetime, timedelta
from textwrap import dedent

from airflow import DAG

from airflow.operators.bash import BashOperator

with DAG(
    'hw_arse-beljaev_5',
    default_args={
        'depends_on_past': False,
        'email': ['airflow@example.com'],
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 1,
        'retry_delay': timedelta(minutes=5)
        },
    description='hw_5_lesson_11',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2023, 9, 17),
    catchup=False,
    tags=['example']
) as dag:

    for i in range(10):

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
            bash_command=f"echo {i}",
            bash_command=templated_command
        )

        t1
