from datetime import datetime

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator


def print_date(ds, **kwargs) -> None:
    print(f"Received ds: {ds}")
    print("Additional message")


default_args = {
    "start_date": datetime(2022, 1, 1),
}

dag = DAG(
    dag_id="my_dag",
    default_args=default_args,
    description="A simple tutorial DAG",
    schedule="@daily",
)

bash_task = BashOperator(
    task_id="bash_task",
    bash_command="pwd",
    dag=dag,
)

python_task = PythonOperator(
    task_id="python_task",
    python_callable=print_date,
    dag=dag,
)

bash_task >> python_task
