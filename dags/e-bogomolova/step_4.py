from datetime import timedelta
from textwrap import dedent

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

with DAG(
    'dynamic tasks',
    default_args={
        'depends_on_past': False,
        'email': ['airflow@example.com'],
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 1,
        'retry_delay': timedelta(minutes=5),
    },
    description='DAG for step 3',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2022, 3, 18),
    catchup=False,
    tags=['step 3'],
) as dag:

    for i in range(10):
        task_b = BashOperator(
            task_id='echo_' + str(i),
            bash_command=f'echo {i}',
        )

    task_b.doc_md = dedent(
        """\
    # Task with BashOperator
    This **task** implements *printing* 10 consecutive numbers
    with the command `echo`

    """
    )

    def print_task_number(task_number):
        print(f'task number is: {task_number}')

    for i in range(20):
        task_p = PythonOperator(
            task_id='task_number_' + str(i),
            python_callable=print_task_number,
            op_kwargs={'task_number': i},
        )
        task_b >> task_p

    task_p.doc_md = dedent(
        """\
    # Task with PythonOperator
    This **task** implements *printing* 10 consecutive numbers
    in the form `task number is: {task_number}`

    """
    )
