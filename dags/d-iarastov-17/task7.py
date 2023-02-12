from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python_operator import PythonOperator
from textwrap import dedent



default_args = {
    'depends_on_past': False,
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),  
}

with DAG(
    'hw_6_d-iarastov-17',
    default_args=default_args,
    schedule_interval=timedelta(days=1),
    start_date=datetime(2023, 2, 9),
    catchup=False,
) as dag:

    for i in range(10):
        t1 = BashOperator(
            task_id=f"print_echo_{i}",
            bash_command=f"echo {i}",
        )


    def print_task_number(task_number, ts, run_id):
        print(task_number, ts, run_id)

    time = '{{ ts }}'
    run_id = '{{ run_id }}'
    for i in range(20):
        t2 = PythonOperator(
            task_id=f"print_task_number_{i}",
            python_callable=print_task_number,
            op_kwargs={'task_number': i, 'ts': time, 'run_id': run_id},
            )
