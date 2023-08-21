"""
# Documentation for DAG **"hw_m-golovaneva_task3"**
"""

from datetime import timedelta, datetime
from textwrap import dedent

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

with DAG(
        "hw_m-golovaneva_task3",

        default_args={
            'depends_on_past': False,
            'email': ['airflow@example.com'],
            'email_on_failure': False,
            'email_on_retry': False,
            'retries': 1,
            'retry_delay': timedelta(minutes=5),
        },
        description='my DAG for 3d task Lecture 11',
        schedule_interval=timedelta(days=1),
        start_date=datetime(2022, 1, 1),
        catchup=False,

        tags=['task3_L11']
) as dag:

    # добавить в BashOperator переменную окружения NUMBER, чье значение будет равно i из цикла
    for i in range(1, 11):
        task_BO = BashOperator(
            task_id=f"Bash_task_{i}",
            bash_command=f"echo $NUMBER",
            env={"NUMBER": i})

        task_BO.doc_md = dedent(
            """
            ## First 10 tasks with **BashOperator**
            - indeed, no one needs these tasks :)
            - but the dont get bored... bcs *there re 10 of them*!

            ```python
            print("Hi reader, we re tasks!")
            ```
            """)


    def print_t_number(task_number: int):
        print(f"task number is: {task_number}")


    for i in range(11, 31):
        task_PO = PythonOperator(
            task_id=f"Python_task_{i}",
            python_callable=print_t_number,
            op_kwargs={"task_number": i}
        )
        task_PO.doc_md = dedent(
            """
            ## 20 more tasks with **PythonOperator**!

            ```python
            print("Hi reader, we re tasks!")
            ```
            """
        )

    dag.doc_md = __doc__

    task_BO >> task_PO