from datetime import datetime, timedelta
from textwrap import dedent

from airflow import DAG

from airflow.decorators import task
from airflow.operators.bash import BashOperator
from airflow.operators.python_operator import PythonOperator


with DAG(
    'hw_7_a-pichugin-23',
    default_args={
        'depends_on_past': False,
        'email': ['airflow@example.com'],
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 1,
        'retry_delay': timedelta(minutes=5),
    },
    start_date=datetime.now(),
    tags=['hw_7_a-pichugin-23'],
) as dag:

    for i in range(10):
        bash_task = BashOperator(
            task_id=f'Bash_task_{i}',
            bash_command=f'echo $NUMBER',
            env={'NUMBER': i}
        )
        bash_task

    for i in range(20):
        def task_py(task_number, ts, run_id):
            print(f'task number is: {task_number}')
            print(ts)
            print(run_id)

        python_task_7 = PythonOperator(
            task_id=f'Python_task_{i}',
            python_callable=task_py,
            op_kwargs={'task_number': i}
            )
        python_task_7