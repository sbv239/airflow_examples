from datetime import datetime, timedelta
from textwrap import dedent

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

with DAG(
    'hw_p-ratibor-20_3',
    start_date=datetime(2023, 5, 11),
    default_args={
        'depends_on_past': False,
        'email': ['airflow@example.com'],
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 1,
        'retry_delay': timedelta(minutes=5),
    },
) as dag:
    dag.doc_md = (
    """
    # Test documentation added by 
    `dag.doc_md = `
    **bold text**
    *italic text*
    ***very important information***
    `code`
    `pointless_task == True`
    some text
    """)


    for i in range(1, 11):
        bash_task = BashOperator(
            task_id=f'task{i}',
            bash_command=f"echo {i}"
        )

        bash_task.doc_md = (
            """
            # Test documentation added by 
            `bash_task.doc_md = `
            **bold text**
            *italic text*
            ***very important information***
            `code`
            `pointless_task == True`
            some text
            """
        )

    def print_task_number(task_number):
        print("task number is: {task_number}")
    
    for i in range(11, 31):
        python_task = PythonOperator(
            task_id=f'task{i}',
            python_callable=print_task_number,
            op_kwargs={'n': {i}}
        )

        bash_task.doc_md = (
            """
            # Test documentation added by 
            `python_task.doc_md = `
            **bold text**
            *italic text*
            ***very important information***
            `code`
            `pointless_task == True`
            some text
            """
        )