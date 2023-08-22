from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta
from textwrap import dedent

def print_i(task_number, **kwargs):
    print(f"task number is: {task_number}")

with DAG(
    'hw_so-kuzmina_4',
    default_args={
    'depends_on_past': False,
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),  # timedelta из пакета datetime
}, description = 'dag with for',
    schedule_interval = timedelta(days = 1),
    start_date = datetime(2023, 8, 20)
) as dag:
    for i in range(30):
        if i < 10:
            t1 = BashOperator(
                task_id = f'bash_task_{i}',
                bash_command = f'echo {i}'
            )
        else:
            t2 = PythonOperator(
                task_id = f'python_task_{i}',
                python_callable = print_i,
                op_kwargs = {'task_id': f'{i}'}
            )

    t1.doc_md = dedent(
        """\
    ##### Task Documentation
    You can document your task using the attributes `doc_md` (markdown),
    `doc` (plain text), `doc_rst`, which gets rendered in the UI's Task Instance 
    Details page.         
    I am *Sonya*
    I am **Sonya**
        """
    )

    dag.doc_md = __doc__