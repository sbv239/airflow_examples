from datetime import datetime, timedelta
from textwrap import dedent

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator


with DAG(
    'k-kavitsjan-18_task_5',
    default_args={
        'depends_on_past': False,
        'email': ['airflow@example.com'],
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 1,
        'retry_delay': timedelta(minutes=5),
        },
    description='task_5',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2023, 3, 20),
    catchup=False,
    tags=['k-kavitsjan-18'],
    ) as dag:

    templated_command = dedent(
        """{% for i in range(5)%}
            echo "{{ts}}"
            echo "{{run_id}}"
        {%endfor%}"""
    )

    task = BashOperator(
        task_id='bash_command_and_Jinja',
        bash_command=templated_command,
    )

    task