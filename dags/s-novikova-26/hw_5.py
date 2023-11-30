"""
HW 5
"""
from datetime import datetime, timedelta
from textwrap import dedent
from airflow import DAG
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
    'hw_5_s-novikova-26',
    default_args=default_args,
    description='HW 5',
    start_date=datetime(2023, 11, 30),
    catchup=False,
    tags=['HW 5']
) as dag:
    templated_command = dedent(
    """
    {% for i in range(5) %}
        echo "{{ ts }}"
        {% endfor %}
        echo "{{ run_id}}"
        """
    ) 
    t1 = BashOperator(
        task_id='echo_ts_run_id',
        depends_on_past=False,
        bash_command=templated_command
        )