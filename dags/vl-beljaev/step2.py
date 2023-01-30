from datetime import timedelta

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
