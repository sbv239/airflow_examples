from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python_operator import PythonOperator
from textwrap import dedent

with DAG(
    # !!!!!!!!!!!!!!!!!!!!!!!!!!!
    'hw_5_a-kalinkin',#МЕНЯЙ ИМЯ ДАГА
    # !!!!!!!!!!!!!!!!!!!!!!!!!!!
    default_args={
        'depends_on_past': False,
        'email': ['airflow@example.com'],
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 1,
        'retry_delay': timedelta(minutes=5),
    },

    description='Second DAG',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2022, 1, 1),
    catchup=False,
    tags=['hw_a-kalinkin'],
) as dag:


        command = dedent(
                """
            {% for i in range(5) %}
                echo "{{ ts }}"
                echo "{{ run_id }}"
            {% endfor %}
            """
        )

        t1 = BashOperator(
                task_id='print_ts_run_id',
                depends_on_past=False,
                bash_command= command
        )