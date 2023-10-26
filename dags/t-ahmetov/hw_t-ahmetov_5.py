from datetime import datetime, timedelta

from airflow import DAG

from airflow.operators.bash import BashOperator
from airflow.operators.python_operator import PythonOperator
from textwrap import dedent

def print_context(ts=None, **kwargs):
    print(ts)


with DAG(
    'task_5_ahmetov',
    # Параметры по умолчанию
    default_args={
        'depends_on_past': False,
        'email': ['airflow@example.com'],
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 1,
        'retry_delay': timedelta(minutes=5),
    },
    start_date=datetime.now(),
) as dag:

    templated_command = dedent(
            """
            {% for i in range(5) %}
                echo {{ ts }}
                echo {{ run_id }}
            {% endfor %}
            """
    )

    t1 = BashOperator(
        task_id='hw_5',
        bash_command=templated_command,
    )
    
    t1
