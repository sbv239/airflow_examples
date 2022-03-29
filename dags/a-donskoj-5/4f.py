from datetime import datetime, timedelta
from textwrap import dedent

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

with DAG(
        'hw_5_a-donskoj-5',
        default_args={
            'depends_on_past': False,
            'email': ['airflow@example.com'],
            'email_on_failure': False,
            'email_on_retry': False,
            'retries': 1,
            'retry_delay': timedelta(minutes=5),  # timedelta из пакета datetime
        },
        description='DAG in 5 step',
        schedule_interval=timedelta(days=1),
        start_date=datetime(2022, 3, 25),
        catchup=False,
        tags=['task 4'],
) as dag:
    command = dedent(
        """
    {% for i in range(5) %}
        echo "{{ ts }}"
    {% endfor %}
    echo "{{ run_id }}"    
    """
    )

    t1 = BashOperator(
        task_id='new_bash_command',
        bash_command=command
    )

    t1