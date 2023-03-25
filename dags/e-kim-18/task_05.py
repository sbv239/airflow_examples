"""
Task 05 documentation
"""
from datetime import datetime, timedelta
from textwrap import dedent
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator



with DAG(
    'e-kim-18_task_05',
    default_args={
        'depends_on_past': False,
        'email': ['airflow@example.com'],
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 1,
        'retry_delay': timedelta(minutes=5),  # timedelta из пакета datetime
    },
    description='A DAG for task 02',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2023, 3, 20),
    catchup=False,
    tags=['e-kim-18-tag'],
) as dag:

    templated_command = dedent(
        """
        {% for i in range(5) %}
            echo "{{ ts }}"
            echo "{{ run_id }}"
        {% endfor %}
    """
    )  # поддерживается шаблонизация через Jinja

    t1 = BashOperator(
        task_id='templated_command',  # id, будет отображаться в интерфейсе
        depends_on_past=False,
        bash_command=templated_command,  # какую bash команду выполнить в этом таске
    )

    t1