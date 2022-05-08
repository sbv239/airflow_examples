from airflow import DAG

from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

from datetime import timedelta, datetime
from textwrap import dedent

with DAG(
        'hw_4_o-beljakov-7',
        default_args={
            'depends_on_past': False,
            'email': ['airflow@example.com'],
            'email_on_failure': False,
            'email_on_retry': False,
            'retries': 1,
            'retry_delay': timedelta(minutes=5),  # timedelta из пакета datetime
        },
        description='hw_4_o-beljakov-7',
        schedule_interval=timedelta(days=1),
        start_date=datetime(2022, 5, 8),
        catchup=False,
        tags=['task_4']
        ) as dag:
        templated_command = dedent(
            """
                {% for i in range(5) %}
                    echo "{{ ts }}"
                {% endfor %}
                echo "{{ run_id }}"
            """
        )

        t3 = BashOperator(
            task_id='templated',
            depends_on_past=False,
            bash_command=templated_command,
        )

        t3