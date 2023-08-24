from datetime import datetime, timedelta
from textwrap import dedent

from airflow import DAG

from airflow.operators.bash import BashOperator
from airflow.operators.python_operator import PythonOperator


def print_iter(task_number, **kwargs):
    print(kwargs)
    return f'task number is: {task_number}'


with DAG(
        'hw_n-shishkin_6',
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
        tags=['hw_n-shishkin_6'],
) as dag:
    for i in range(10):
        t = BashOperator(
            task_id='bash_' + str(i + 1),
            env={"NUMBER": str(i + 1)},
            bash_command="echo $NUMBER"
        )