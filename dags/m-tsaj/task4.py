from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from textwrap import dedent

with DAG(
        'dag_5_m-tsaj',
        default_args={
            'depends_on_past': False,
            'email': ['airflow@example.com'],
            'email_on_failure': False,
            'email_on_retry': False,
            'retries': 1,
            'retry_delay': timedelta(minutes=5),
        },
        description='Template tasks dag',
        schedule_interval=timedelta(days=1),
        start_date=datetime(2021, 3, 20),
        catchup=False,
) as dag:
    templated_command = dedent(
        """
        {% for i in range(5) %}
            echo "{{ ts }}"
        {% endfor %}
        echo "{{ run_id }}"
        """
    )

    t1 = BashOperator(
        task_id='template_command_print',
        bash_command=templated_command,
    )

    t1
