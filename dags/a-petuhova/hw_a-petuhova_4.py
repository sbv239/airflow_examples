from datetime import datetime, timedelta
from textwrap import dedent

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

with DAG(
        'a-petuhova_step5',
        default_args={
            'depends_on_past': False,
            'email': ['airflow@example.com'],
            'email_on_failure': False,
            'email_on_retry': False,
            'retries': 1,
            'retry_delay': timedelta(minutes=5),  # timedelta из пакета datetime
        },
        description='Tasks for step 5 a-petuhova',
        schedule_interval=timedelta(days=1),
        start_date=datetime(2023, 5, 23),
        catchup=False,
        tags=['a-petuhova'],
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
        task_id=f'task_bash',
        bash_command=templated_command
    )