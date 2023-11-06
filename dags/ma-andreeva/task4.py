
from datetime import datetime, timedelta
from textwrap import dedent
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.operators.dummy import DummyOperator

with DAG(
    'task_4_andreeva',
    default_args={
        'depends_on_past': False,
        'email': ['airflow@example.com'],
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 1,
        'retry_delay': timedelta(minutes=5),
    },
    description='A DAG for task 4',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2023, 10, 30),
    catchup=False,
    tags=['task4','task_4','andreeva'],
) as dag:

    templated_command = dedent (
        """
        {% for i in range (5) %}
            echo "{{ts}}"
            echo "{{run_id}}"
        {% endfor %}
        """

    )

    task_bash = BashOperator(
        task_id='print_templated_command',
        bash_command=templated_command,
    )


