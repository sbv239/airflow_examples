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

def print_task_number(ts, run_id, **kwargs):
    print(f"task number is: {kwargs['task_number']}")
    print(f"ts = {ts}")
    print(f"run_id = {run_id}")

with DAG(
    'hw_alekse-smirnov_7',
    default_args=default_args,
    description='DAG for Lesson #11 Task #7',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2023, 10, 15),
    catchup=False,
    tags=['alekse-smirnov'],
) as dag:
    for i in range(1, 21):
        ptask = PythonOperator(
            task_id=f"step_{i}_python",
            python_callable=print_task_number,
            op_kwargs={"task_number": i}
        )
        ptask.doc_md = dedent(
            """\
            ### This is python task.

            This task print nomber of it using `print_task_number()` **python** cfunction.\

            _This juast test task._
            """
        ) 
