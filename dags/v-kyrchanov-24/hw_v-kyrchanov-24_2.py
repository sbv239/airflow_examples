from datetime import timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator


def print_date(ds, **kwargs) -> None:
    print(f"Received ds: {ds}")
    print("Additional message")


default_args = {
    "depends_on_past": False,
    "email": ["airflow@example.com"],
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),  # timedelta из пакета datetime
}

dag = DAG(
    dag_id="my_dag",
    default_args=default_args,
    description="A simple tutorial DAG",
    schedule_interval="@daily",
)

bash_task = BashOperator(
    task_id="bash_task",
    bash_command="pwd",
    dag=dag,
)

python_task = PythonOperator(
    task_id="python_task",
    python_callable=print_date,
    provide_context=True,
    dag=dag,
)

bash_task >> python_task
