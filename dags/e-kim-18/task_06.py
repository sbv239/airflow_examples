"""
Task 02 documentation
"""
from datetime import datetime, timedelta
from textwrap import dedent
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator



with DAG(
    'task_06',
    default_args={
        'depends_on_past': False,
        'email': ['airflow@example.com'],
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 1,
        'retry_delay': timedelta(minutes=5),  # timedelta из пакета datetime
    },
    description='A DAG for task 02',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2023, 3, 20),
    catchup=False,
    tags=['e-kim-18-tag'],
) as dag:

    for i in range(10):
        t1 = BashOperator(
            task_id=f'echo_command_{i}',  # id, будет отображаться в интерфейсе
            bash_command=f"echo $NUMBER",  # какую bash команду выполнить в этом таске
            env={"NUMBER": str(i)},  # задает переменные окружения
        )
    t1