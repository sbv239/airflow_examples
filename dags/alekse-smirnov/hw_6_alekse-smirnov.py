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

def print_task_number(task_number):
    print(f"task number is: {task_number}")

with DAG(
    'hw_alekse-smirnov_6',
    default_args=default_args,
    description='DAG for Lesson #11 Task #6',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2023, 10, 14),
    catchup=False,
    tags=['alekse-smirnov'],
) as dag:
    for i in range(1, 11):
        btask = BashOperator(
            task_id= f"step_{i}_bash",
            bash_command=f"echo $NUMBER",
            env={"NUMBER": i}
        )
        btask.doc_md = dedent(
            """\
            ### This is bush task.

            This task print nomber of it using `echo` **bash** command.\

            _This juast test task._
            """
        ) 
