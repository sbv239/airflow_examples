from datetime import datetime, timedelta
from textwrap import dedent
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from textwrap import dedent

default_args = {
    'depends_on_past': False,
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    'hw_alekse-smirnov_5',
    default_args=default_args,
    description='DAG for Lesson #11 Task #5',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2023, 10, 17),
    catchup=False,
    tags=['alekse-smirnov'],
) as dag:
    templated_command = """
    {% for i in range(5) %}
        echo {{ ts }}
        echo {{ run_id }}
    {% endfor %}
    """
    btask = BashOperator(
            task_id= "step_templted_bash",
            bash_command=templated_command
        )
