from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.dummy import DummyOperator
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from textwrap import dedent

with DAG(
    "hw_3_n_vojtova",
    default_args={
        'depends_on_past': False,
        'email': ['airflow@example.com'],
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 1,
        'retry_delay': timedelta(minutes=5),  # timedelta из пакета datetime
    },
    description='DAG with loop',
    schedule_interval= timedelta(days=1),
    start_date=datetime(2023,12,7),
    catchup=False,
    tags=['hw_3','n_vojtova'],
) as dag:

    t1 = DummyOperator(task_id ="start_dag")
    t2 = DummyOperator(task_id ="wait_for_all_operators")
    t3 = DummyOperator(task_id ="end_dag")

    for i in range(10):
        task = BashOperator(
            task_id=f'print_date_{i}',
            bash_command=f'echo {i}',
        )
        task.doc_md = dedent(
            """
        ###Task Documentation###
        **Print Date:** {task.doc_md}**
        Printing 10 times       
            """
        )

        t1 >> task >> t2
    def task_nos(number, **kwargs):
        print(kwargs)
        print(f"task number is: {kwargs.get('number')}")
        return "task number"

    for i in range(20):
        py_task = PythonOperator(
            task_id=f"print_number_{i}",
            python_callable=task_nos,
            op_kwargs={'number': i},
        )
        py_task.doc_md = dedent(
            """
        ###Task Documentation###
        *Could receive a number between 1 and 'range' (from 1 to 20)*
        Receive number with help `op_kwargs`       
            """
        )

        t2 >> py_task >> t3