from datetime import timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python_operator import PythonOperator


with DAG(
    default_args={
        "depends_on_past": False,
        "email": ["airflow@example.com"],
        "email_on_failure": False,
        "email_on_retry": False,
        "retries": 1,
        "retry_delay": timedelta(minutes=5),
    },
    catchup=False,
) as dag:

    def print_ds(ds, **kwargs):
        print(ds)

    t1 = PythonOperator(task_id="ds_printer", python_callable=print_ds)
    t2 = BashOperator(task_id="pwd_printer", bash_command="pwd ")
