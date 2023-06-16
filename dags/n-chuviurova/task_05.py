from textwrap import dedent
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator

with DAG(
        "hw_5_n-chuviurova",
        default_args={
            "depends_on_past": False,
            "email": ["airflow@example.com"],
            "email_on_failure": False,
            "email_on_retry": False,
            "retries": 1,
            "retry_delay": timedelta(minutes=5)
        },
        description="templates",
        schedule_interval=timedelta(days=1),
        start_date=datetime(2023, 6, 15),
        catchup=False,
        tags=["task_05"],
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
        task_id="hw_n-chuviurova_templated",
        bash_command=templated_command,
    )

    t1