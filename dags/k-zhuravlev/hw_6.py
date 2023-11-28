from datetime import datetime, timedelta
from textwrap import dedent

# Для объявления DAG нужно импортировать класс из airflow
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator


with DAG(
    "task_6_k-zhuravlev",

default_args={
    'depends_on_past': False,
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),  # timedelta из пакета datetime
},

    start_date=datetime.now(),
    tags=["Cool_tag"]
) as dag:

    # templated_bash_command = dedent(
    #     """"
    # {% for i in range(10) %}
    #     f"echo {i}"
    # {% endfor %}
    # """
    # )

    for task_number in range(10):
        bash_task = BashOperator(
            task_id=f'Bash_task_{task_number}',
            bash_command=f'echo $NUMBER',
            env={"NUMBER": task_number}
        )
        bash_task

    bash_task.doc_md = dedent(
        """"
        `code`, **bold**, *italic*, regular
        # Header
        """
    )
    # templated_python_command = dedent(
    #     """"
    # {% for i in range(10) %}
    #     f"echo {i}"
    # {% endfor %}
    # """
    # )
    def print_iteration(task_number):
        print(task_number)
        return "Epshtein didn't kill himself"

    for task_number in range(20):
        python_task = PythonOperator(
            task_id=f"python_interation_{task_number}",
            python_callable=print_iteration,
            op_kwargs={task_number}
        )
        python_task

    python_task.doc_md = dedent(
        """"
        `code`, **bold**, *italic*, regular
        # Header
        """
    )