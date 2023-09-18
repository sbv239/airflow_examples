from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta
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
        dag_id="hw_aleksandraleksand-ivanov_5",
        default_args=default_args,
        start_date=datetime(2023, 9, 18)
) as dag:
    templated_command = dedent("""
    {% for i in range(5) %}
        echo "{{ts}}"
        echo "{{run_id}}"
    {% endfor %}
    """)

    t1 = BashOperator(
        task_id="jinja_task",
        bash_command=templated_command
    )

    t1
