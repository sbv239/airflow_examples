"""
# Foo
Hello, these are DAG docs.
"""
from textwrap import dedent

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

from datetime import timedelta, datetime

with DAG(
    "the_hw_7",
    default_args={
        'depends_on_past': False,
        'email': ['airflow@example.com'],
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 1,
        'retry_delay': timedelta(minutes=5),  # timedelta из пакета datetime
    },
    description='oh_my_DAG',
    schedule_interval=timedelta(days=1),
    start_date=datetime(year=2022, month=3, day=22),
    catchup=False,
    tags=['hw_7'],
) as dag:

    def print_context(task_n: int, ts, run_id, **kwargs):
        print(f'task number is: {task_n}')
        print(ts)
        print(run_id)

    for task_n in range(1, 21):
        t1 = PythonOperator(
            task_id='task_number_' + str(i),
            python_callable=print_context,
            op_kwargs={'task_number': task_n},
        )

        t1

