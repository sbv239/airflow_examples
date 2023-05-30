from airflow import DAG
from datetime import timedelta, datetime

from airflow.operators.bash import BashOperator

from textwrap import dedent


with DAG(
        'hw_l-anoshkina_5',

        default_args={
        'depends_on_past': False,
                           'email': ['airflow@example.com'],
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 1,
        'retry_delay': timedelta(minutes=5),  # timedelta из пакета datetime
        },
        description = 'HomeWork task2',
        schedule_interval = timedelta(days=1),
        start_date = datetime(2023, 5, 29),

        catchup = False,

        ) as dag:
    templated_command = dedent(
        """
        {% for i in range(5) %}
        echo "{{ ts }}"
        echo "{{ run_id }}"
        {% endfor %}
        """
    )
    task = BashOperator(
        task_id='templated',
        depends_on_past=False,
        bash_command=templated_command
    )

task