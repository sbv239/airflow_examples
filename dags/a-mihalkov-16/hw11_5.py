from datetime import datetime, timedelta
from textwrap import dedent
from airflow import DAG
from airflow.operators.bash import BashOperator


with DAG(
    'Task_5',
    default_args={
        'depends_on_past': False,
        'email': ['airflow@example.com'],
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 1,
        'retry_delay': timedelta(minutes=5),
    },
    start_date=datetime.now(),
    tags=['Task_5'],
) as dag:

    bash_command = dedent(
            """
            {% for i in range(5) %}
                echo {{ ts }}
                echo {{ run_id }}
            {% endfor %}
            """
    )

    t1 = BashOperator(
            task_id='HW_5',
            bash_command=bash_command
    )

    t1
