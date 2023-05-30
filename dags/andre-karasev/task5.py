from datetime import timedelta, datetime
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow import DAG
from textwrap import dedent


default_args = {
        'depends_on_past': False,
        'email': ['airflow@example.com'],
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 1,
        'retry_delay': timedelta(minutes=5),  # timedelta из пакета datetime
    }
with DAG(
        'andre-karasev_hw_5',
        default_args=default_args,
        description='hw_5_',
        schedule_interval=timedelta(days=1),
        start_date=datetime(2022, 9, 9),
        catchup=False,
        tags=['andre-karasev_hw_5']
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
                task_id='templated_command',
                bash_command=templated_command,
        )
