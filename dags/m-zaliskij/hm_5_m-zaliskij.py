from datetime import timedelta, datetime
from textwrap import dedent

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

with DAG(
        'hw_5_m-zaliskij',
        default_args={
            'depends_on_past': False,
            'email': ['airflow@example.com'],
            'email_on_failure': False,
            'email_on_retry': False,
            'retries': 1,
            'retry_delay': timedelta(minutes=5)
        },
        start_date=datetime(2022, 1, 1),
        catchup=False

) as dag:
    t1 = BashOperator(
            task_id="echo",
            bash_command=dedent(
                '''
            {% for i in range(5) %}
                echo "{{ts}}"
                echo "{{run_id}}"
            {% endfor %}
            '''
            )
        )
