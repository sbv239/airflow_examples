from datetime import datetime, timedelta
from textwrap import dedent

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python_operator import PythonOperator

with DAG(
        'HW_3_d-koh',
        default_args={
            'depends_on_past': False,
            'email': ['airflow@example.com'],
            'email_on_failure': False,
            'email_on_retry': False,
            'retries': 1,
            'retry_delay': timedelta(minutes=5),
        },
        description='DAG from step 3',
        schedule_interval=timedelta(days=1),
        start_date=datetime(2022, 2, 10),
        catchup=False,
        tags=['first'],
) as dag:
    for i in range(10):
        task_bash = BashOperator(
            task_id='print_task' + str(i),
            bash_command=f"echo {i}",
        )

    def print_task(task_number):
        print(f'task number is: {task_number}')


    for i in range(20):
        t2 = PythonOperator(
            task_id='task_number_' + str(i),
            python_callable=print_task,
            op_kwargs={'task_number': i}
        )

    task_bash >> t2
