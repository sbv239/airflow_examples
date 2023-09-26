from datetime import datetime, timedelta
from textwrap import dedent

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.models import Variable


def print_context(ts, run_id, **kwargs):
    print("Task number:", kwargs["task_number"])
    print("ts:", ts)
    print("run_id:", run_id)


with DAG(
    "hw_a-ulyanov_3",
    default_args={
        "depends_on_past": False,
        "email": ["airflow@example.com"],
        "email_on_failure": False,
        "email_on_retry": False,
        "retries": 1,
        "retry_delay": timedelta(minutes=5),
    },
    description="hw_3 DAG",
    start_date=datetime(2023, 9, 23),
    schedule_interval=timedelta(days=1),
    catchup=False,
) as dag:
    bash_tasks = []
    python_tasks = []

    for i in range(10):
        t1 = BashOperator(
            task_id=f"echo_{i}", bash_command=f"echo $NUMBER", env={"NUMBER": str(i)}
        )
        bash_tasks.append(t1)

    for i in range(20):
        t2 = PythonOperator(
            task_id=f"print_{i}_task_number",
            python_callable=print_context,
            op_kwargs={"task_number": i},
            provide_context=True,
        )
        python_tasks.append(t2)

    for i in range(1, len(bash_tasks)):
        bash_tasks[i - 1] >> bash_tasks[i]

    for i in range(1, len(python_tasks)):
        python_tasks[i - 1] >> python_tasks[i]
