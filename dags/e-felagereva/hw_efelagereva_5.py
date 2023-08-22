from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta
from textwrap import dedent

with DAG(
    'hw_efelagereva_5',
    default_args={
    'depends_on_past': False,
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),  # timedelta из пакета datetime
}, description = "dag which has a format",
    schedule_interval = timedelta(days = 1),
    start_date = datetime(2023, 8, 21)
) as dag:
    templated_command = dedent(
        """
        {% for i in range(4) %}
        echo "{{ts}}"
        echo "{{run_id}}"
        {% endfor %}
        """
    )

    t1 = BashOperator(
        task_id = 'templated',
        bash_command = templated_command
    )

