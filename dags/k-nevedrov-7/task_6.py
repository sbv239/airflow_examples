from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python_operator import PythonOperator

with DAG(
    "k-nevedrov-7-task_6",
    default_args={
        "depends_on_past": False,
        "email": ["airflow@example.com"],
        "email_on_failure": False,
        "email_on_retry": False,
        "retries": 1,
        "retry_delay": timedelta(minutes=5),  # timedelta из пакета datetime
    },
    schedule_interval=timedelta(days=1),
    start_date=datetime(2022, 1, 1),
    catchup=False,
) as dag:
    def print_context(task_number, ts, run_id, **kwargs):
        print(f"task number is: {task_number}")
        print(ts)
        print(run_id)

    p_t = [
        PythonOperator(
            task_id=f"print_task_number_{i}",
            python_callable=print_context,
            op_kwargs={"task_number": i},
        )
        for i in range(20)
    ]
