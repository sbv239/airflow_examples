from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
from textwrap import dedent

with DAG(
    'hw_al-pivovarov_5',
    default_args={
        'depends_on_past': False,
        'email': ['airflow@example.com'],
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 1,
        'retry_delay': timedelta(minutes=5),  # timedelta из пакета datetime
    },
    start_date=datetime.now()
) as dag:
    templated_command = dedent("""
        {% for i in range(5) %}
            echo "{{ts}}"
            echo "{{run_id}}"
        {% endfor %}
    """)
    t1 = BashOperator(task_id='templated', bash_command=templated_command)