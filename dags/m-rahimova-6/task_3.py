from airflow import DAG
from datetime import timedelta, datetime
from textwrap import dedent

from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

with DAG(
    'rakhimova_task3',
    # Параметры по умолчанию для тасок
    default_args={
        'depends_on_past': False,
        'email': ['airflow@example.com'],
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 1,
        'retry_delay': timedelta(minutes=5),  # timedelta из пакета datetime

    },
    description='DAG3 Rakhimova',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2022, 3, 25),
    catchup=False,
    tags=['hehe'],
) as dag:

    def print_task_nubmer(task_number):
        print(f'task number is: {task_number}')

    for i in range(10):
        t_bash = BashOperator(
                 task_id=f'bash_task_{i}',
                 bash_command=f'echo {i}',
        )

        t_bash.doc_md = dedent(
            """
            #Bash operator documentation
            **prints** number `i` of a *task*
            """
        )


    for j in range(20):
        t_python = PythonOperator(
                task_id=f'python_task_{j}',
                python_callable=print_task_nubmer,
                op_kwargs={'task_number': j},
        )

        t_python.doc_md = dedent(
            """
            #Python operator documentation
            **prints** number `i` of a *task*
            """
        )
