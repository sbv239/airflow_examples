from datetime import datetime, timedelta
from textwrap import dedent
from airflow import DAG

from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator


with DAG(
    'kretov',
    default_args={
        'depends_on_past': False,
        'email': ['airflow@example.com'],
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 1,
        'retry_delay': timedelta(minutes=5),
    },
    description='first DAG',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2023, 11, 20),
    catchup=False,
    tags=["hw_5"]
) as dag:
    
    task1 = BashOperator(
        task_id='print_values',
        bash_command=dedent('''
            {% for i in range(5) %}
                echo "Value of ts: {{ ts }}"
                echo "Value of run_id: {{ run_id }}"
            {% endfor %}
        '''),
    )
