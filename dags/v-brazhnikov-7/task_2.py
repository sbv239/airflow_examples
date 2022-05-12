from datetime import timedelta

from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.operators.bash import BashOperator
from airflow.operators.python_operator import PythonOperator


with DAG(
    "task_1",
    default_args={
        "depends_on_past": False,
        "email": ["airflow@example.com"],
        "email_on_failure": False,
        "email_on_retry": False,
        "retries": 1,
        "retry_delay": timedelta(minutes=5),
        "start_date": days_ago(2),
    },
    catchup=False,
) as dag:

    for i in range(10):
        BashOperator(task_id=f"echo_task_{i}", bash_command=f"echo {i}", dag=dag)

    def print_ds(task_number, **kwargs):
        print(f"task number is: {task_number}")

    for task_number in range(11, 30):
        PythonOperator(
            task_id=f"ds_printer_{i}",
            python_callable=print_ds,
            op_kwargs={"task_number": task_number},
            dag=dag,
        )
