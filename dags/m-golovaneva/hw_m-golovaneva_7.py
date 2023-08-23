"""
# Documentation for DAG **"hw_m-golovaneva_task3"**
"""

from datetime import timedelta, datetime
from textwrap import dedent

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

with DAG(
        "hw_m-golovaneva_task7",

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

        tags=['mariaSG']
) as dag:

    # какие переменные содержаться в **kwargs
    def my_func(**kwargs):
        print("Variables:", kwargs)

    kwargs_check = PythonOperator(
        task_id="kwargs_check",
        python_callable=my_func
    )

    def print_t_number(**kwargs):
        task_number = kwargs['task_number']
        ts = kwargs['ts']
        run_id = kwargs['run_id']
        print(f"Task number: {task_number}, ts: {ts}, run_id: {run_id}")

    for i in range(11, 31):
        task_PO = PythonOperator(
            task_id=f"Python_task_{i}",
            python_callable=print_t_number,
            op_kwargs={"task_number": i},
            #provide_context=True
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

    kwargs_check >> task_PO