from datetime import datetime, timedelta
from textwrap import dedent

from airflow import DAG

from airflow.operators.bash import BashOperator

# from airflow.operators.python import PythonOperator

default_args = {
    'depends_on_past': False,
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
        "hw_5_n-efremov",
        default_args=default_args,
        schedule_interval=timedelta(days=1),
        start_date=datetime(2023, 5, 22),
        catchup=False,
        tags=['second_task'],
) as dag:
    templated_command = dedent(
        """
        {% for i in range(5) %}
            echo "{{ ts }}"
            echo "{{ run_id }}"
        {% endfor %}
        """
    )

    task_1 = BashOperator(
        task_id="templated_command",
        depends_on_past=False,
        bash_command=templated_command,
    )

    task_1
