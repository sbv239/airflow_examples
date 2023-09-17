from datetime import datetime, timedelta

from airflow import DAG

from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

with DAG(
    "ti-www_task2",
    default_args={
        'depends_on_past': False,
        'email': ['airflow@example.com'],
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 1,
        'retry_delay': timedelta(minutes=5),
    },
    description="A simple DAG for task 2",
    schedule_interval=timedelta(days=1),
    start_date=datetime(2023, 9, 16),
    catchup=False,
    tags=["ti-www"],
) as dag:

    t1 = BashOperator(
        task_id='print_working_dir',
        depends_on_past=False,
        bash_command='pwd',
    )

    def my_print(ds, **kwargs):
        print(ds)
        print("My name is Timur")
        print(kwargs)

    t2 = PythonOperator(
        task_id='print_the_date_and_my_name',
        python_callable=my_print,
    )

    t1 >> t2