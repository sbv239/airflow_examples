from datetime import timedelta, datetime
from airflow import DAG
from airflow.operators.bash import BashOperator
from textwrap import dedent

with DAG(
        "hw_5_s-kim",
        description="Homework 5",
        schedule_interval=timedelta(days=1),
        start_date=datetime(2023, 8, 1),
        catchup=True,
        tags=["s-kim"],
        default_args={
            'depends_on_past': False,
            'email': ['airflow@example.com'],
            'email_on_failure': False,
            'email_on_retry': False,
            'retries': 1,
            'retry_delay': timedelta(minutes=5)
        }
) as dag:

    templated_command = dedent("""
    {% for i in range(5) %}
        echo "{{ ts }}"
        echo "{{ run_id }}"
    {% endfor %}""")

    t1 = BashOperator(
        task_id="for_loop",
        depends_on_past=False,
        bash_command=templated_command
    )

    t1