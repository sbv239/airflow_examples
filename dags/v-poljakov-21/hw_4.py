from datetime import datetime, timedelta
from textwrap import dedent

from airflow import DAG

from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

def print_task_number(task_number):
    print(f"task number is: {task_number}")

with DAG(
    'hw_v-poljakov-21_4',
    default_args={
        'depends_on_past': False,
        'email': ['airflow@example.com'],
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 1,
        'retry_delay': timedelta(minutes=5)
    },
    description='Exercise 4',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2023, 6, 29),
    catchup=False,
    tags=['hw4'],
) as dag:
    for i in range(0, 10):
        task = BashOperator(
            task_id="echo" + str(i),
            bash_command=f"echo {i}",
            dag=dag
        )

        task.doc_md = dedent("""
        #### Task Documentation

        This is a documentation for **task {i}**.
        It _just execute_ `echo {i}`

        """)

        task

    for i in range(10, 30):
        task = PythonOperator(
            task_id="echo" + str(i),
            python_callable=print_task_number,
            op_kwargs={"task_number":i}
        )
        task

