from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator


def print_ds(ds):
    print(ds)


with DAG(
    "step2",
    default_args={
        "depends_on_past": False,
        "email": ["airflow@example.com"],
        "email_on_failure": False,
        "email_on_retry": False,
        "retries": 1,
        "retry_delay": timedelta(minutes=5),
    },
    start_date=datetime(2023, 1, 1),
    schedule_interval=timedelta(days=1),
    catchup=False,
) as dag:

    task1 = BashOperator(
        task_id="bash_op",
        bash_command="pwd",
    )

    task2 = PythonOperator(
        task_id="python_op",
        python_callable=print_ds,
    )

    task1 >> task2
