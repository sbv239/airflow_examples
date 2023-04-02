"""
Task 3
"""
from datetime import datetime, timedelta

from airflow import DAG

from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

with DAG(
    'hw_3_d-isakov-18',
    default_args={
        'depends_on_past': False,
        'email': ['airflow@example.com'],
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 1,
        'retry_delay': timedelta(minutes=5),
    },
    description=__doc__,
    schedule_interval=timedelta(days=1),
    start_date=datetime(2023, 4, 1),
    catchup=False,
) as dag:

    for i in range(10):
        t1 = BashOperator(
            task_id='command_echo_' + str(i),
            bash_command=f"echo {i}",
        )

    def print_task_number(task_number):
        print(f'task number is: {task_number}')

    for i in range(10, 30):
        t2 = PythonOperator(
            task_id='print_task_number_' + str(i),
            python_callable=print_task_number,
            op_kwargs={'task_number': i},
        )

    t1 >> t2
