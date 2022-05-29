"""
Task 6: https://lab.karpov.courses/learning/84/module/1049/lesson/10040/29383/139291/
"""
from datetime import datetime, timedelta
from textwrap import dedent

from airflow import DAG

from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

default_args={
    'depends_on_past': False,
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    }

with DAG(
    'hw_6_s.zlenko-7',
    default_args = default_args,
    schedule_interval=timedelta(days=1),
    start_date=datetime(2022, 1, 1),
    catchup=False,
) as dag:


    def print_task_number(task_number, ts, run_id):
        print(f'task number is {task_number}')
        print(ts)
        print(run_id)

    for i in range(10, 30):
        t2 = PythonOperator(
            task_id='print_task' + str(i),
            python_callable=print_task_number,
            op_kwargs={'task_number': i}
        )
        t2.doc_md = dedent("""
        # Task documentation
        This task run **python** command `print` the number of times mentioned as argument in *for loop*
        """)

    t2

