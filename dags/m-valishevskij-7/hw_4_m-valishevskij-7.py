from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import timedelta, datetime
from textwrap import dedent

with DAG(
        'hw_4_m-valishevskij-7',
        default_args={
            'depends_on_past': False,
            'email': ['airflow@example.com'],
            'email_on_failure': False,
            'email_on_retry': False,
            'retries': 1,
            'retry_delay': timedelta(minutes=5),
        },
        schedule_interval=timedelta(days=1),
        start_date=datetime(2022, 4, 14),
        catchup=False,
        tags=['valishevskij']
) as dag:
    templated_command = dedent(
        """
        {% for i in range(5) %}
            echo "{{ts}}"
            echo "{{run_id}}"
        {% endfor %}
        """
    )

    task_1 = BashOperator(
        task_id=f'hw_4_m-valishevskij-7_1',
        bash_command=templated_command
    )

