from datetime import datetime, timedelta
from textwrap import dedent
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

with DAG(
    'i-mjasnikov-22_hw_5',
    default_args={
        'depends_on_past': False,
        'email': ['airflow@example.com'],
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 1,
        'retry_delay': timedelta(minutes=5),
    },
    description='5th hw with cycled command',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2023, 2, 10),
    catchup=False,

) as dag:

    cycled_command = dedent(
    """
    {% for i in range(5) %}
        echo "{{ ts }}"
        echo "{{run_id}}"
    {% endfor %}
    """
    )

    task = BashOperator(
        task_id='cycled',
        bash_command=cycled_command
    )
