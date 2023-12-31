from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from textwrap import dedent

from datetime import datetime, timedelta

with DAG(
    'task2_d.timbal',
    default_args={
        'depends_on_past': False,
        'email': ['airflow@example.com'],
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 1,
        'retry_delay': timedelta(minutes=5),
    },
        description='FirstDag',
        schedule_interval=timedelta(days=1),
        start_date=datetime(2023, 1, 30),
        catchup=False,
        tags=['example'],
) as dag:
    for i in range(10):
        t1 = BashOperator(
            task_id='print_circle_' + str(i),
            bash_command=f"echo {i}",
        )

    def print_task_number(task_number):
        print("task number is: {task_number}")

    for i in range(20):
        t2 = PythonOperator(
            task_id='print_task_number' + str(i),
            python_callable=print_task_number,
            op_kwargs={'task_number': i},
        )
    t1.doc_md = dedent(
        """
        # TASK DOCUMENTATION
        this is `code` 
        this is *italic*
        this is **bold**
        
        """
    )
    t1 >> t2
