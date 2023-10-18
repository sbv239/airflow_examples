import datetime

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from textwrap import dedent

from datetime import timedelta


with DAG(
    'hw_p-matchenkov_5',
    default_args={
        'depends_on_past': False,
        'email': ['airflow@example.com'],
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 1,
        'retry_delay': timedelta(minutes=5),
    },
    description='task 5 dag',
    start_date=datetime.datetime(2023, 10, 16),
    catchup=False,
    tags=['matchenkov']
) as dag:

    bash_commands = """
    {% for i in range(5) %}
        echo "{{ ts }}"
        echo "{{ i }}"        
    {% endfor %}
    """

    bash_task = BashOperator(
        task_id='print vars',
        bash_command=bash_commands
    )

    bash_task