from datetime import datetime, timedelta
from textwrap import dedent

# Для объявления DAG нужно импортировать класс из airflow
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator


with DAG(
    "task_7_k-zhuravlev",

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
    for task_number in range(10):
        bash_task = BashOperator(
            task_id=f'Bash_task_{task_number}',
            bash_command=f'echo {task_number}'
        )
        bash_task

    def print_iteration(ts, run_id, **kwargs):
        print(ts)
        print(run_id)
        print(kwargs)
        return "Epshtein didn't kill himself"


    for task_number in range(20):
        python_task = PythonOperator(
            task_id=f"python_interation_{task_number}",
            python_callable=print_iteration,
            op_kwargs={task_number}
        )
        python_task