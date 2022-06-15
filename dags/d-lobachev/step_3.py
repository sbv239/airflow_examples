from datetime import datetime, timedelta
from textwrap import dedent

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python_operator import PythonOperator

with DAG(
        'HW_3_d-lobachev_Dynamic_task',
        default_args={
            'depends_on_past': False,
            'email': ['airflow@example.com'],
            'email_on_failure': False,
            'email_on_retry': False,
            'retries': 1,
            'retry_delay': timedelta(minutes=5),  # timedelta из пакета datetime
        },
        description='DAG step 3 of HW Lesson 11',
        schedule_interval=timedelta(days=1),
        start_date=datetime(2022, 2, 10),
        catchup=False,
        tags=['second'],
) as dag:
    def print_task(task_number):
        print(f'task number is: {task_number}')


    for i in range(1, 31):
        if i < 11:
            bash_op = BashOperator(
                task_id='print_task' + str(i),
                bash_command=f'echo {i}'
            )
        else:
            python_op = PythonOperator(
                task_id='task_number_' + str(i),
                python_callable=print_task,
                op_kwargs={'task_number': i}
            )

    bash_op >> python_op
