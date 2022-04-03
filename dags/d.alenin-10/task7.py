from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from textwrap import dedent

default_args = {
        'depends_on_past': False,
        'email': ['airflow@example.com'],
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 1,
        'retry_delay': timedelta(minutes=5)
    }


with DAG(
    'hw_7_d.alenin-10',
    default_args=default_args,
    description='Simple first dag',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2022, 3, 20),
    catchup=False
) as dag:

    def print_task_number(task_number, ts, run_id):
        print(f"task number is: {task_number}")
        print(f"ts is {ts}")
        print(f"run_id is {run_id}")

    for i in range(10, 30):
        t = PythonOperator(
            task_id=f"print_task_num_{i}",
            python_callable=print_task_number,
            op_kwargs={'task_number': i},
        )
