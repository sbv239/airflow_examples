"""
Task 5
"""
from datetime import datetime, timedelta
from textwrap import dedent

from airflow import DAG

from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

with DAG(
    'hw_5_d-isakov-18',
    default_args={
        'depends_on_past': False,
        'email': ['airflow@example.com'],
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 1,
        'retry_delay': timedelta(minutes=5),
    },
    description=__doc__,
    schedule_interval=timedelta(days=1),
    start_date=datetime(2023, 4, 1),
    catchup=False,
) as dag:

    t1 = BashOperator(
        task_id='bash_template_task',
        bash_command=dedent(
            """
            {% for i in range(5) %}
                echo "{{ts}}"
                echo "{{run_id}}"
            {% endfor %}
            """)
    )

    t1
