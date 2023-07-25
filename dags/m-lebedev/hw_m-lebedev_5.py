from datetime import datetime, timedelta
from airflow import DAG
from textwrap import dedent
from airflow.operators.bash import BashOperator

with DAG(
    'hw_m-lebedev_5',
    default_args={
        'depends_on_past': False,
        'email': ['airflow@example.com'],
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 1,
        'retry_delay': timedelta(minutes=5),
    },
    description='Homework: 5, login: m-lebedev',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2023, 7, 24),
    catchup=False,
    tags=['m-lebedev'],
) as dag:
    
    templated_command = dedent(
        """
    {% for i in range(1,5) %}
        echo "{{ ts }}"
    {% endfor %}
    echo {{ run_id }}
    """
    )
    
    bash_task = BashOperator(
            task_id= 'bash_print',
            bash_command=templated_command,
        )