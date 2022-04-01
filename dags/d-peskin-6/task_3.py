from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

from textwrap import dedent

with DAG(
    'task_3',
    default_args={
        'depends_on_past': False,
        'email': ['airflow@example.com'],
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 1,
        'retry_delay': timedelta(minutes=5)
    },
    description='DAG for task 1',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2022, 3, 25),
    catchup=False,
    tags=['DP HW3']
) as dag:
    for i in range(10):
        bash_task=BashOperator(
                task_id="bash_echo" + str(i),
                bash_command=f'echo {i}'
        )

    bash_task.doc_md = dedent(
        """
        #### Bash task documentation
        10 tasks, executing bash `echo` command 
        """
    )


    def print_task_number(task: int):
        print(f'task number is {task}')


    for i in range(10, 30):
        python_task= PythonOperator(
                task_id="python_task_number" + str(i),
                python_callable=print_task_number,
                op_kwargs={'task': i}
        )

    python_task.doc_md = dedent(
        """
        20 python task just *printing* the task *number*
        """
    )
