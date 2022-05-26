"""
Task 2: https://lab.karpov.courses/learning/84/module/1049/lesson/10040/29383/139287/
"""
from datetime import datetime, timedelta
from textwrap import dedent

from airflow import DAG

from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator


with DAG(
    'hw_2_s.zlenko-7',
    default_args={
    'depends_on_past': False,
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    },
    schedule_interval=timedelta(days=1),
    start_date=datetime(2022, 1, 1),
    catchup=False,
) as dag:

    for i in range(10):
        t1 = BashOperator(
            task_id='echo' + str(i),
            bash_command=f'echo {i}'
        )


    def print_task_number(task_number):
        print(f'task number is {task_number}')

    for i in range(10, 30):
        t2 = PythonOperator(
            task_id='print_task' + str(i),
            python_callable=print_task_number,
            op_kwargs={'task_number': i}
        )

    t1 >> t2
