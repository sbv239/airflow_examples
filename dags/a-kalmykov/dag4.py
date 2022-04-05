from datetime import datetime, timedelta
from textwrap import dedent
from airflow import DAG

from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

default_args = {
    'depends_on_past': False,
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}


def print_task_num(task_num):
    print(f'python task number is {task_num}')


with DAG(
        dag_id='a-kalmykov-dag-4',
        default_args=default_args,
        description='Dag 4 Kalmykov',
        schedule_interval=timedelta(days=1),
        start_date=datetime(2022, 4, 4),
        catchup=False,
        tags=['a-kalmykov'],
) as dag:

    templated_command = dedent(
        """
    {% for i in range(5) %}
        echo "ts={{ ts }}"
        echo "run_id={{ run_id }}"
    {% endfor %}
    """)

    t = BashOperator(
        task_id='templated',
        depends_on_past=False,
        bash_command=templated_command,
    )

    t