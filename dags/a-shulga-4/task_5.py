from airflow import DAG
from datetime import datetime, timedelta
from airflow.operators.bash import BashOperator
from airflow.operators.python_operator import PythonOperator
from textwrap import dedent

with DAG(
    'hw_a-shulga-4_5',
    default_args={
        'depends_on_past': False,
        'email': ['airflow@example.com'],
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 1,
        'retry_delay': timedelta(minutes=5), 
    },
    schedule_interval=timedelta(days=1),
    start_date=datetime(2022, 6, 27),
    catchup=False
    ) as dag:
       templated_command = dedent(
                """
                {% for i in range(5) %}
                    echo {{ ts }}
                    echo {{ run_id }}
                {% endfor %}
                """
       )
       t = BashOperator(
                   task_id = 'hw_5_task',
                   bash_command = templated_command
                )
       
       t