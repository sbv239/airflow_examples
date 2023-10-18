import datetime

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

from datetime import timedelta


def print_task_number(**kwargs):
    print(kwargs)


with DAG(
    'hw_p-matchenkov_3',
    default_args={
        'depends_on_past': False,
        'email': ['airflow@example.com'],
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 1,
        'retry_delay': timedelta(minutes=5),
    },
    description='task 3 dag',
    start_date=datetime.datetime(2023, 10, 16),
    catchup=False,
    tags=['matchenkov']
) as dag:

    for i in range(10):
        bash_task = BashOperator(
            task_id=f'print {i}',
            bash_command=f'echo {i}'
        )

    for i in range(20):
        python_task = PythonOperator(
            task_id='current_task_{0}'.format(i),
            python_callable=print_task_number,
            op_kwargs={'number': i}
        )

    bash_task >> python_task