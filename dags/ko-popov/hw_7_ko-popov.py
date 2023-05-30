from datetime import datetime, timedelta
from textwrap import dedent
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator



def task_count(task_number, ts, run_id, **kwargs):
    print(f"task number is {task_number}")
    print(f"ts is {ts}")
    print(f"run_id is {run_id}")

with DAG(
    'hw_ko-popov_7',
    default_args={
        'depends_on_past': False,
        'email': {'mdkonstantinp@gmail.com'},
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 1,
        'retry_delay': timedelta(minutes=5)
    },
    description='hw_7_ko-popov dag',
    schedule_interval = timedelta(days=1),
    start_date=datetime(2023, 5, 29),
    catchup=False,
    tags = ['hw_ko-popov_7'],
) as dag:
    for i in range(30):
        if i < 10:
            task = BashOperator(
                task_id = f"Bash_task_{i}",
                bash_command = f"echo {i}"
            )
        else:
            task = PythonOperator(
                task_id = f"Python_task_{i}",
                python_callable = task_count,
                op_kwargs = {"task_number": i, "ts": "{{ ts }}", "run_id": "{{ run_id }}"}
            )