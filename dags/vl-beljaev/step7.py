from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator


def print_task_info(ts, run_id, task_number):
    print(f"ts is {ts}")
    print(f"run_id is {run_id}")
    print(f"task_number is {task_number}")


with DAG(
    "step6",
    default_args={
        "depends_on_past": False,
        "email": ["airflow@example.com"],
        "email_on_failure": False,
        "email_on_retry": False,
        "retries": 1,
        "retry_delay": timedelta(minutes=5),
    },
    schedule_interval=timedelta(days=1),
    start_date=datetime(2023, 1, 1),
    catchup=False,
) as dag:
    for i in range(10, 30):
        PythonOperator(
            task_id=f"python_op_{i}",
            python_callable=print_task_info,
            op_kwargs={"task_number": i},
        )
