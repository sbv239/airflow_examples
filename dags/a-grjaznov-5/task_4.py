from datetime import datetime, timedelta
from textwrap import dedent

from airflow import DAG

from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
with DAG(
    'task_4_grjaznov',
        default_args={
            'depends_on_past': False,
            'email': ['airflow@example.com'],
            'email_on_failure': False,
            'email_on_retry': False,
            'retries': 1,
            'retry_delay': timedelta(minutes=5)
        },
    description='task_4_DAG',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2022, 3, 31),
    catchup=False,
    tags=['hw_4_a-grjaznov-5'],
) as dag:
    templated_command = dedent(
        """
        {% for i in range(5) %}
            echo "{{ ds }}"
            {% endfor %}
        echo "{{run_id}}"
    """
    )
    t1 = BashOperator(
        task_id='bash_babash',
        bash_command= templated_command
        )
