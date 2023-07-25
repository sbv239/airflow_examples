from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator

with DAG(
    'hw_m-lebedev_6',
    default_args={
        'depends_on_past': False,
        'email': ['airflow@example.com'],
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 1,
        'retry_delay': timedelta(minutes=5),
    },
    description='Homework: 6, login: m-lebedev',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2023, 7, 24),
    catchup=False,
    tags=['m-lebedev'],
) as dag:

    for i in range(1, 11):
        bash_task = BashOperator(
            task_id= f'bash_print_task_{i}',
            env={"NUMBER": i},
            bash_command="echo $NUMBER",
        )