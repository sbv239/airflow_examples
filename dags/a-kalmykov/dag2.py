"""
#### Dag 2
"""
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
        dag_id='a-kalmykov-dag-2',
        default_args=default_args,
        description='Dag 2 Kalmykov',
        schedule_interval=timedelta(days=1),
        start_date=datetime(2022, 4, 4),
        catchup=False,
        tags=['a-kalmykov'],
) as dag:
    tasks = []
    for idx in range(30):
        if idx < 20:
            task = BashOperator(
                task_id=f'bash_echo_{idx}',
                bash_command=f'echo task number is {idx}',
            )
        else:
            task = PythonOperator(
                task_id=f'python_print_{idx}',
                python_callable=print_task_num,
                op_kwargs={'task_num': idx}
            )
        task.doc_md = dedent(
            """
        #### Print Task Number
        prints number of the current task
        """
        )
        tasks.append(task)


    dag.doc_md = __doc__

    # tasks[0] >> tasks[1:]
    tasks