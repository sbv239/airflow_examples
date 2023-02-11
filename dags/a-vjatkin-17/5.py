from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
from textwrap import dedent


default_args = {
    'depends_on_past': False,
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}


with DAG(
    'a-vjatkin-17_task_5',
    default_args=default_args,
    description='a-vjatkin-17_task_5',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2023, 2, 11),
    catchup=False
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
        task_id='templated',
        depends_on_past=False,
        bash_command=templated_command,
    )
