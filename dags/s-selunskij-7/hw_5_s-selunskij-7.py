from datetime import datetime, timedelta
from textwrap import dedent

from airflow import DAG

from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

with DAG(
        's-selunskij-7_task_4',
        # Параметры по умолчанию для тасок
        default_args={
            'depends_on_past': False,
            'email': ['airflow@example.com'],
            'email_on_failure': False,
            'email_on_retry': False,
            'retries': 1,
            'retry_delay': timedelta(minutes=5),
        },
        description='s-selunskij-7_DAG_task_4',
        schedule_interval=timedelta(days=1),
        start_date=datetime(2022, 4, 4),
        catchup=False,
        tags=['task_4'],
) as dag:
    templated_command = dedent(
        """
            {% for i in range(5) %}
                echo "{{ ts }}"
            {% endfor %}
            echo "{{ run_id }}"

        """
    )

    t3 = BashOperator(
        task_id='templated',
        depends_on_past=False,
        bash_command=templated_command,
    )

    t3