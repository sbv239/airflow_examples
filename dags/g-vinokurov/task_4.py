from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
from textwrap import dedent

with DAG(
    'hw_g-vinokurov_4',
    default_args={
        'depends_on_past': False,
        'email': ['airflow@example.com'],
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 1,
        'retry_delay': timedelta(minutes=5),
    },
        description='DAG in task_4',
        schedule_interval=timedelta(days=1),
        start_date=datetime(2023, 9, 28),
        catchup=False,
) as dag:


    my_command = dedent(
        """
    {% for i in range(5) %}
        echo "{{ ts }}"
        echo "{{ run_id }}"
    {% endfor %}
    """
    )

    operator_1 = BashOperator(
        task_id=f'Bash_operator',
        bash_command=my_command,
    )