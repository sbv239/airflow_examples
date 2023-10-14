from datetime import datetime, timedelta
from textwrap import dedent
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator


with DAG(
    'hw_5_a-korotaeva',
    default_args={
        'depends_on_past': False,
        'email': ['airflow@example.com'],
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 1,
        'retry_delay': timedelta(minutes=5),
    },
    description='A simple tutorial DAG', schedule_interval=timedelta(days=1),
    start_date=datetime(2022, 10, 13),
    catchup=False
) as dag:

    temp_command = dedent(
        """"
    {% for i in range(5) %}
        echo "{{ts}}"
        echo "{{task_id}}"
    {% endfor %}
    """
    )

    t1 = BashOperator(task_id='some_bash_comands', bash_command=temp_command)

    t1