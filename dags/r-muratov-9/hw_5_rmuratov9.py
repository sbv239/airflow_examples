from datetime import datetime, timedelta
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow import DAG
from textwrap import dedent

with DAG(

    'hw_5_r-muratov-9',

    default_args={
    'depends_on_past': False,
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),  # timedelta из пакета datetime
    },

    schedule_interval=timedelta(days=1),

    start_date=datetime(2023,5,20),

    catchup=False
) as dag:
    
    templated_command = """
    {% for i in range(5) %}
        echo "{{ ts }}"
        echo "{{ run_id }}"
    {% endfor %}
    """

    run_tasks = BashOperator(
        task_id='hw_5_rmuratov9',
        bash_command=templated_command
    )

    run_tasks
