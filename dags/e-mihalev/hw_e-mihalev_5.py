from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
from textwrap import dedent

with DAG(
'hw_e-mihalev_4',
    default_args={
        'depends_on_past': False,
        'email': ['airflow@example.com'],
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 1,
        'retry_delay': timedelta(minutes=5),  # timedelta из пакета datetime
    },
    description='A simple DAG',
    start_date=datetime(2023, 10, 15),
    catchup=False,
    tags=['example']
) as dag:
    templated_command = dedent(
        """
        {% for i in range(5) %}
            echo {{ ts }}
            echo {{ run_id }}
        {% endfor %}
        """
    )
    task = BashOperator(
        task_id="task",
        bash_command=templated_command,
        dag = dag
    )
    task