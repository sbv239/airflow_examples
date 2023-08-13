from datetime import timedelta, datetime
from textwrap import dedent
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

with DAG(
        "hw_3_s-kim",
        description="Homework 3",
        schedule_interval=timedelta(days=1),
        start_date=datetime(2023, 8, 1),
        catchup=True,
        tags=["s-kim"],
        default_args={
            'depends_on_past': False,
            'email': ['airflow@example.com'],
            'email_on_failure': False,
            'email_on_retry': False,
            'retries': 1,
            'retry_delay': timedelta(minutes=5)
        }
) as dag:
    def python_func(task_number):
        print("task number is: " + str(task_number))


    for i in range(10):
        t1 = BashOperator(
            task_id="bash_" + str(i),
            bash_command="echo " + str(i)
        )

        # Add docs
        t1.doc_md = """
        # Docs for the first task
        Apply function `echo` using **BashOperator** and *for* loop"""

        t1


    for i in range(20):
        t2 = PythonOperator(
            task_id="python_" + str(i),
            python_callable=python_func,
            op_kwargs={"task_number": i}
        )

        # Add docs
        t2.doc_md = """
        # Docs for the second task
        Apply `for` loop and **PythonOperator** printing some *text*"""

        t2