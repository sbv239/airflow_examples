from datetime import datetime, timedelta
from textwrap import dedent
from airflow import DAG
from airflow.operators.bash import BashOperator

with DAG(
    'hw_5_i-kulikov-17',
    default_args={
        'depends_on_past': False,
        'email': ['airflow@example.com'],
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 1,
        'retry_delay': timedelta(minutes=5),
    },
    description='cycled_command',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2023, 2, 10),
    catchup=False,
    tags=['hw_5_lawfirmfacc@mail.ru'],

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