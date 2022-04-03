from datetime import datetime, timedelta
from textwrap import dedent
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator


with DAG(
    'hw_3_s-hodzhabekova-6',
    default_args={
        'depends_on_past': False,
        'email': ['airflow@example.com'],
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 1,
        'retry_delay': timedelta(minutes=5),
    },
    description='A simple tutorial DAG',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2022, 1, 1),
    catchup=False,
    tags=['hw_3_s-hodzhabekova-6', 'khodjabekova'],
) as dag:

    tc = dedent(
        """
        {% for i in range(5) %}
            echo "{{ts}}"
        {% endfor %}
        echo "{{run_id}}"
    """
    )

    task = BashOperator(
        task_id='templated',
        bash_command=tc,
    )

    task
        