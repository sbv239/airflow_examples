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

    logical_date = "{{ ts }}"
    current_DAG_run = "{{ run_id }}"

    def print_t_number(task_number, ts, run_id, **kwargs):
        print(f"task number is: {task_number}")
        print(f"logical date is: {logical_date}")
        print(f"current DAG run is: {current_DAG_run}")

    for i in range(11, 31):
        task_PO = PythonOperator(
            task_id=f"Python_task_{i}",
            python_callable=print_t_number,
            op_kwargs={"task_number": i,
                       "ts": logical_date,
                       "run_id": current_DAG_run},
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