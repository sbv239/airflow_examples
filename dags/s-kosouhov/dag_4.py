from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from datetime import timedelta, datetime
from textwrap import dedent

default_args={
    'depends_on_past': False,
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),  # timedelta из пакета datetime
}

with DAG(
    'task_4',
    default_args=default_args,
    schedule_interval=timedelta(days=1),
    start_date=datetime(2022, 5, 10),
    catchup=False,
    tags=['example_2'],
) as dag:

    templated_command = dedent(
        """
        {% for i in range(5) %}
            echo "{{ts}}"
            echo "{{run_id}}"
        {% endfor %}
        """
    )
    
    task = BashOperator(
            task_id = "templated",
            bash_command = templated_command
           )
    task