from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from datetime import datetime, timedelta
from textwrap import dedent

# Set arguments for the DAG
with DAG(
    'hw_so-kuzmina_5',
    default_args={
    'depends_on_past': False,
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),  # timedelta из пакета datetime
}, description = 'da',
    schedule_interval = timedelta(days = 1),
    start_date = datetime(2023, 8, 20)
) as dag:
    templated_command = dedent(
        """
        {% for i in range(5) %}
            echo "{{ ts }}"
            echo "{{ run_id }}"
        {% endfor %}
        """
    )

    templated_task = BashOperator(
        task_id='templated_task',
        depends_on_past=False,
        bash_command=templated_command,
    )