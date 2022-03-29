from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

from datetime import timedelta, datetime
from textwrap import dedent


def print_task(task_number):
    print(f"task number is: {task_number}")


with DAG(
    "hw_2_3_shamil.utaraptor",
    default_args={
        'depends_on_past': False,
        'email': ['airflow@example.com'],
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 1,
        'retry_delay': timedelta(minutes=5),  # timedelta из пакета datetime
    },
    description='DAG for the second and third homework',
    schedule_interval=timedelta(days=1),
    start_date=datetime(year=2022, month=3, day=22),
    catchup=False,
    tags=['hw_2_3_shamil.utaraptor'],
) as dag:
    for i in range(1, 31):
        if i <= 10:
            task = BashOperator(
                task_id=f'print_loop_iteration_{i}',  # id, будет отображаться в интерфейсе
                bash_command=f'echo {i}',  # какую bash команду выполнить в этом таске
            )
            task.doc_md = dedent(
                """\
                ### Task documentation
                Runs bash command `echo` to print cycle iteration.
                Cycle starts from **one**. bash operator is used in first *ten* iterations.
                """
            )
        else:
            task = PythonOperator(
                task_id=f"print_task_number_{i}",
                python_callable=print_task,
                op_kwargs={'task_number': i}
            )
            task.doc_md = dedent(
                """\
                ### Task documentation
                Runs Python function to print cycle iteration.
                Cycle starts from **one**. Python is used after first *ten* iterations.
                """
            )
    task