from datetime import datetime, timedelta
from textwrap import dedent

from airflow import DAG

from airflow.operators.bash import BashOperator
from airflow.operators.python_operator import PythonOperator


with DAG(
        'hw_n-shishkin_5',
        default_args={
            'depends_on_past': False,
            'email': ['airflow@example.com'],
            'email_on_failure': False,
            'email_on_retry': False,
            'retries': 1,
            'retry_delay': timedelta(minutes=5),  # timedelta из пакета datetime
        },
        schedule_interval=timedelta(days=1),
        start_date=datetime(2022, 1, 1),
        catchup=False,
        tags=['hw_n-shishkin_5'],
) as dag:
    template=dedent("""
    {% for i in range(5) %}
        echo "{{ts}}"
        echo "{{run_id}}"
    {% endfor %}
    """)
    t=BashOperator(
        task_id='templated_bash',
        bash_command=template
    )
