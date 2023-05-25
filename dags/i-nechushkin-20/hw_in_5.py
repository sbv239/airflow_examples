"""
KC Lesson 11 Airflow
Task 5
"""
from datetime import datetime, timedelta
from textwrap import dedent

from airflow import DAG

from airflow.operators.bash import BashOperator
from airflow.operators.python_operator import PythonOperator


with DAG(
    'Task_5',
    # DAG dafault parameters
    default_args={
        'depends_on_past': False,
        'email': ['airflow@example.com'],
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 1,
        'retry_delay': timedelta(minutes=5),
        'owner': 'i-nechushkin-20'
    },
    start_date=datetime.now(),
    tags=['i-nechushkin-20_Task_5'],
) as dag:

    templated_command = dedent(
        """
    {% for i in range(5) %}
        echo "{{ ts }}"
        echo "{{ run_id }}"
    {% endfor %}
    """
    ) # шаблонизация через Jinja
    t = BashOperator(
        task_id=f'templated',
        bash_command=templated_command,
    )

    t.doc_md = dedent(
    """
    ## Task **«t1»** documentation
    This task is repeated *10* times using a `for` loop.
    """
    )

    t
