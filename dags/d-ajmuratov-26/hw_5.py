"""
Hello, Airflow Documentation!!!
"""
from textwrap import dedent

from datetime import timedelta
from datetime import datetime

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

default_args={
    'depends_on_past': False,
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),  # timedelta из пакета datetime
}

with DAG(
    dag_id='hw_d-ajmuratov-26_5',
    default_args=default_args,
    start_date=datetime.now(),
    catchup=False,
    tags=['HM5']
) as dag:
    templated_command = dedent(
    """
    {% for i in range(5) %}
        echo "{{ ts }}"
        echo "{{ run_id }}"
    {% endfor %}
    """
    )
    t = BashOperator(
        task_id='print_with_jinja',
        bash_command=templated_command
    )
    t