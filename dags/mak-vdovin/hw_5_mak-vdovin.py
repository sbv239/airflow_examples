from datetime import datetime, timedelta

from airflow import DAG

from airflow.operators.bash import BashOperator
from textwrap import dedent

with DAG(
    'templatization',
    default_args={
        'depends_on_past': False,
        'email': ['airflow@example.com'],
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 1,
        'retry_delay': timedelta(minutes=5),
    },
    description='home work "templatization"',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2023, 9, 28),
    catchup=False,
    tags=['hw_5'],
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
        task_id='templated',
        depends_on_past=False,
        bash_command=templated_command,
    )

    if __name__ == "__main__":
        dag.test()