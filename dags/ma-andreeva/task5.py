
from datetime import datetime, timedelta
from textwrap import dedent
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.operators.dummy import DummyOperator

with DAG(
    'task_5_andreeva',
    default_args={
        'depends_on_past': False,
        'email': ['airflow@example.com'],
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 1,
        'retry_delay': timedelta(minutes=5),
    },
    description='A DAG for task 5',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2023, 10, 30),
    catchup=False,
    tags=['task5','task_5','andreeva'],
) as dag:

    for i in range(10):
        task_bash = BashOperator(
            task_id=f'print_bash_{i}',
            bash_command='echo  "env_number: $NUMBER" ',
            env={"NUMBER": str(i)},
        )





