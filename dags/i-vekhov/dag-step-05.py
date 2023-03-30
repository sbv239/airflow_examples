from airflow.operators.bash import BashOperator
from airflow import DAG
from datetime import datetime, timedelta
from textwrap import dedent


with DAG(
    'hw_5_i-vekhov',
    default_args={
        'depends_on_past': False,
        'email': ['airflow@example.com'],
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 1,
        'retry_delay': timedelta(minutes=5),  # timedelta из пакета datetime
    },
    description='hw_3_i-vekhov',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2021, 3, 29),
    catchup=False,
    tags=['hw_5_i-vekhov'],
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
        task_id='templated_hw_5',
        depends_on_past=False,
        bash_command=templated_command
    )
    t

