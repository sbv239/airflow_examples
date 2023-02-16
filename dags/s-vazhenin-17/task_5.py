from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta

from textwrap import dedent


with DAG(
        's-duzhak-2-task_4',
        default_args={
            'depends_on_past': False,
            'email': ['airflow@example.com'],
            'email_on_failure': False,
            'email_on_retry': False,
            'retries': 1,
            'retry_delay': timedelta(minutes=5),
        },
        description='A simple tutorial DAG',
        schedule_interval=timedelta(days=1),
        start_date=datetime(2023, 2, 15),
        catchup=False,
        tags=['example'],
) as dag:
    templated_command = dedent(
        """
        {% for i in range(5) %}
        echo "{{ ts }}"
        echo "{{ run_id }}"
        {% endfor %}
        """)
    t3 = BashOperator(
        task_id = 'task_4_bash',
        depends_on_past=False,
        bash_command=templated_command)
    