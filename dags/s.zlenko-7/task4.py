"""
Task 4: https://lab.karpov.courses/learning/84/module/1049/lesson/10040/29383/139289/
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
    'hw_4_s.zlenko-7',
    default_args=default_args,
    schedule_interval=timedelta(days=1),
    start_date=datetime(2022, 1, 1),
    catchup=False,
) as dag:

    templated_command = dedent(
    """
    {% for i in range(5) %}
        print '{{ ts }}'
        print '{{ run_id }}
    """
    )

    t1 = BashOperator(
        task_id='print_templated_command',
        bash_command=templated_command
    )


