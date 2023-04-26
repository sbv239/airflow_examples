"""
Test documentation
"""
from datetime import datetime, timedelta
from textwrap import dedent

from airflow import DAG

from airflow.operators.python_operator import PythonOperator
from airflow.operators.bash import BashOperator

with DAG(
    'hw_5_a_loskutov',
    # Параметры по умолчанию для тасок

    default_args={
        'depends_on_past': False,
        'email': ['airflow@example.com'],
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 1,
        'retry_delay': timedelta(minutes=5),  # timedelta из пакета datetime
    },

    description='My first DAG',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2023, 4, 25),
    catchup=False,
    tags=['Loskutov_hw'],
) as dag:
    

    command = dedent(
            """
        {% for i in range(5) %}
            echo "{{ ts }}"
            echo "{{ run_id }}"
        {% endfor %}
        """
    )

    t1_Bash = BashOperator(
            task_id='print_ts_run_id',
            depends_on_past=False,
            bash_command= command
    )

    
