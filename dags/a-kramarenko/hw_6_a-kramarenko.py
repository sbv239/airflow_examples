from datetime import datetime, timedelta
from airflow import DAG

from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

with DAG(
    'hw_a-kramarenko_6',
    default_args={
        'depends_on_past': False,
        'email': ['airflow@example.com'],
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 1,
        'retry_delay': timedelta(minutes=5),
    },
    description='lesson 11 task 6 DAG',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2023, 10, 26),
) as dag:

    for i in range(10):
        task1 = BashOperator(
            task_id='print_the_context' + str(i),
            bash_command="echo $NUMBER",
            env={"NUMBER": str(i)},
        )
        task1
