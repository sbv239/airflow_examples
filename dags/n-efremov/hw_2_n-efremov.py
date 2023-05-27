from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

with DAG(
        "hw_n-efremov_2",
        default_args={
            'depends_on_past': False,
            'email': ['airflow@example.com'],
            'email_on_failure': False,
            'email_on_retry': False,
            'retries': 1,
            'retry_delay': timedelta(minutes=5),
        },
        description="hw_n-efremov_2 DAG",
        schedule_interval=timedelta(days=1),
        start_date=datetime(2023, 5, 22),
        catchup=False,
        tags=['second_task'],
) as dag:
    t1 = BashOperator(
        task_id='print pwd',
        bash_command="pwd",
    )


    def print_context(ds, **kwargs):
        print(kwargs)
        print(ds)
        return "SOME TEXT"


    t2 = PythonOperator(
        task_id='print_i',
        python_callable=print_context,
    )

    t1 >> t2
