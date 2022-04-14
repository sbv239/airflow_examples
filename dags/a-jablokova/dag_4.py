from datetime import datetime, timedelta
from textwrap import dedent

from airflow import DAG

from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

with DAG(
    'intro_4th',
    default_args={
        'depends_on_past': False,
        'email': ['airflow@example.com'],
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 1,
        'retry_delay': timedelta(minutes=5), 
    },
    description='intro_4th',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2022, 3, 10),
    catchup=False,
    tags=['a-jablokova'],
) as dag:

    
    templated_command = dedent(
        """
        {% for i in range(5) %}
            echo "{{ts}}"
            echo "{{run_id}}"
        {% endfor %}
        """
    )

    t1 = BashOperator(
        task_id = 'templated',
        bash_command = templated_command,
    )

    t1