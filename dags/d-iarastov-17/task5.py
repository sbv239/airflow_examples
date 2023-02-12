from datetime import datetime, timedelta
from airflow import DAG
from textwrap import dedent

from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

default_args={
    'depends_on_past': False,
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),  
}

with DAG(
    'hw_5_d-iarastov-17',
    default_args = default_args,
    start_date=datetime(2023, 2, 1),
    description = "templated_command with bash loop"
) as dag:
    templated_command = dedent(
    """
    {% for i in range(5) %}
    echo "{{ ts }}"
    echo "{{ run_id }}"
    {% endfor %}
    """
    )

    t1 = BashOperator(
    task_id='hw_5_d-iarastov-17',
    bash_command=templated_command,
    )

   
    t1


