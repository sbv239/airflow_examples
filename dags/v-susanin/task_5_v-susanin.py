from datetime import datetime, timedelta
from textwrap import dedent

from airflow import DAG

from airflow.operators.bash import BashOperator

with DAG(
    'v-susanin_task_5',
    default_args={
        'depends_on_past': False,
        'email': ['airflow@example.com'],
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 1,
        'retry_delay': timedelta(minutes=5),
    },
    description='v-susanin_DAG_task_5',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2023, 1, 26),
    catchup=False,
    tags=['DAG_task_5'],
) as dag:
    templated_code = dedent(
        """
        {% for i in range(5) %}
        echo "{{ ts }}"
        {% endfor %}
        echo "{{ run_id }}"
        """)
    b = BashOperator(
        task_id='command',
        bash_command=templated_code)

