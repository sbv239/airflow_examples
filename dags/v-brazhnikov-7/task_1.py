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

    def print_ds(ds, **kwargs):
        print(ds)

    python_task = PythonOperator(
        task_id="ds_printer", python_callable=print_ds, dag=dag
    )
    bash_task = BashOperator(task_id="pwd_printer", bash_command="pwd ", dag=dag)

    bash_task >> python_task
