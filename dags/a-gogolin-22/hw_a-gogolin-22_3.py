"""
Test documentation
"""
from datetime import datetime, timedelta
from textwrap import dedent

from airflow import DAG

from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator

with DAG(
        'hw_a-gogolin-22_3',
        default_args={
            'depends_on_past': False,
            'email': ['airflow@example.com'],
            'email_on_failure': False,
            'email_on_retry': False,
            'retries': 1,
            'retry_delay': timedelta(minutes=5),
        },
        description='simple bash dag',
        schedule_interval=timedelta(days=1),
        start_date=datetime(2023, 7, 27),
        catchup=False,
        tags=['ag_3'],
) as dag:
    
    bash_tasks = []
    for i in range(10):
        task = BashOperator(
            task_id='echo__' + str(i),
            bash_command=f"echo {i}"
        )
        bash_tasks.append(task)

    python_tasks = []
    def print_task_number(task_number, **kwargs):
        print(f"task number is: {task_number}")


    for i in range(20):
        task = PythonOperator(
            task_id='task__' + str(i),
            python_callable=print_task_number,
            op_kwargs={'task_number': i},
            provide_context=True
        )
        python_tasks.append(task)

    for i in range(1, 10):
        bash_tasks[i-1] >> bash_tasks[i]

    for i in range(1, 20):
        python_tasks[i-1] >> python_tasks[i]

    bash_tasks[-1] >> python_tasks
