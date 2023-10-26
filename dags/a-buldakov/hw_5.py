from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta
from textwrap import dedent

default_args={
    'depends_on_past': False,
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),  # timedelta из пакета datetime
}

dag = DAG(
    'hw_5_a-buldakov',
    default_args = default_args,
    start_date = datetime.now(),
    tags=['a-buldakov']
)

templated_command = dedent(
    """
    {% for i in range(5) %}
        echo {{ ts }}
        echo {{ run_id }}
    {% endfor %}
    """
)

hw5_bash_task = BashOperator(
    task_id = 'hw5_bash_task',
    bash_command=templated_command,
    dag = dag
)

hw5_bash_task