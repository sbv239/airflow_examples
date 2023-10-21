from datetime import datetime, timedelta
from textwrap import dedent
from airflow import DAG
from airflow.operators.bash import BashOperator, PythonOperator


with DAG(
    "hw_11_ex_2-n-jazvinskij",
    default_args={
        "depends_on_past": False,
        "email": ["airflow@example.com"],
        "email_on_failure": False,
        "email_on_retry": False,
        "retries": 1,
        "retry_delay": timedelta(minutes=5),
    },
    description="ex_2-n-jazvinskij",
    schedule=timedelta(days=1),
    start_date=datetime(2023, 10, 21),
    catchup=False,
    tags=["hw_11_ex_2"],
) as dag:
        t1 = BashOperator(
                task_id = 'print_directory',
                bash_command = 'pwd'
        )
        def print_ds (ds, **kwargs):
                print(ds)
                print(kwargs)
                return 'ds_was_printed'

        t2 = PythonOperator(
                task_id = 'print_python_operator',
                python_calleble = print_ds
        )
        t1 >> t2