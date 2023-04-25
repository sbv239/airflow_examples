"""
step_7 DAG
"""
from datetime import datetime, timedelta

from airflow import DAG

from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator


with DAG(
    'hw_r-shahvaly_7',

    default_args={
        'depends_on_past': False,
        'email': ['airflow@example.com'],
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 1,
        'retry_delay': timedelta(minutes=5),
    },
    description='DAG for step_7',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2023, 1, 1),
    catchup=False,
    tags=['r-shahvaly'],
) as dag:

    def print_task_number(task_number, ts, run_id):
        print(task_number)
        print(ts)
        print(run_id)

    task_prev = BashOperator(
        task_id='bash_echo_0',
        bash_command=f'echo $NUMBER',
        env={"NUMBER": str(0)}
    )

    for task_number in range(1, 10):
        task = BashOperator(
            task_id=f"bash_pwd_{task_number}",
            bash_command=f'echo $NUMBER',
            env={"NUMBER": str(task_number)}
        )
        task_prev >> task
        task_prev = task

    for task_number in range(10, 30):
        task = PythonOperator(
            task_id=f"python_print_{task_number}",
            python_callable=print_task_number,
            op_kwargs = {"task_number": task_number}
        )
        task_prev >> task
        task_prev = task