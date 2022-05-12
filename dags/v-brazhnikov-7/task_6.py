from datetime import timedelta

from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.operators.bash import BashOperator
from airflow.operators.python_operator import PythonOperator


with DAG(
    "task_6",
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

    def print_ds(ts, run_id, task_number, **kwargs):
        print(ts)
        print(run_id)

    for task_number in range(11, 30):
        PythonOperator(
            task_id=f"ds_printer_{task_number}",
            python_callable=print_ds,
            op_kwargs={"task_number": task_number},
            dag=dag,
        )
