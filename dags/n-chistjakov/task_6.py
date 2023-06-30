from datetime import datetime, timedelta

from airflow import DAG
from textwrap import dedent

from airflow.operators.bash import BashOperator
from airflow.operators.python_operator import PythonOperator

ID = "hw_6_n-chistjakov_"

with DAG(
    'hw_6_n-chistjakov',
    default_args={
    'depends_on_past': False,
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    },
    description="Second task",
    start_date=datetime(2023, 6, 30),
    schedule_interval=timedelta(days=1),
    catchup=False,
    tags=["task_06"],
) as dag:

    for i in range(10):
        bash = BashOperator(
            task_id=ID + f"_{i}",
            bash_command="echo $NUMBER",
            env={"NUMBER": i}
        )

    bash