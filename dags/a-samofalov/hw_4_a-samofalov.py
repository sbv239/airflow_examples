from datetime import datetime, timedelta
from textwrap import dedent
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

with DAG(
    'HW_3_a-samofalov',
    default_args={
            'depends_on_past': False,
            'email': ['airflow@example.com'],
            'email_on_failure': False,
            'email_on_retry': False,
            'retries': 1,
            'retry_delay': timedelta(minutes=5),
        },
    description='A simple tutorial DAG june 2023',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2022, 1, 1),
    catchup=False,
    tags=['hw_3_a-samofalov'],
) as dag:
    def print_task(task_number):
        return print(f'task number is: {task_number}')

    for i in range(30):
        if i <= 9:
            bash_task = BashOperator(
                task_id='bash_print_' + str(i),  # в id можно делать все, что разрешают строки в python
                bash_command= f"echo {i}"
            )
            bash_task.doc_md = dedent("""\
                #### Task Documentation
                You **can document** your *task* using ___the___ attributes `doc_md` (markdown),
                """)
        else:
            python_task = PythonOperator(
                task_id=f'python_command_{i}',
                python_callable=print_task,
                op_kwargs={'task_number': i}
            )
            python_task.doc_md = dedent("""\
                #### Task Documentation
                You **can document** your *task* using ___the___ attributes `doc_md` (markdown),
                """)
    bash_task >> python_task

