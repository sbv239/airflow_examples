from datetime import timedelta, datetime
from textwrap import dedent
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

default_args={
    'depends_on_past': False,
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

def print_task_number(task_number):
    print(f'task number is: {task_number}')

with DAG(
    'rag_hw_4',
    description='HW_step_4',
    default_args=default_args,
    schedule_interval=timedelta(days=1),
    start_date=datetime(2023, 4, 1),
    catchup=False,
    tags=['rag23'],
) as dag:

    dag.doc_md = dedent(
        """
        Документация на DAG:
        # тестовый абзац
        `code`
        **полужирный**
        *курсив*
        """
    )

    for i in range(30):
        if i < 10:
            task = BashOperator(
                task_id = f'bash_task_{i}',
                bash_command = f'echo {i}'
            )
        else:
            task = PythonOperator(
                task_id = f'python_task_{i}',
                python_callable=print_task_number,
                op_kwargs = {'task_number': i}
            )   
        task.doc_md = dedent(
            """
            Документация на task:
            # тестовый абзац
            `code`
            **полужирный**
            *курсив*
            """)        
        task
