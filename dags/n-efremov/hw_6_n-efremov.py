from datetime import datetime, timedelta
from textwrap import dedent
from airflow import DAG
from airflow.operators.bash import BashOperator

default_args = {
    'depends_on_past': False,
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
        "hw_6_n-efremov",
        default_args=default_args,
        schedule_interval=timedelta(days=1),
        start_date=datetime(2023, 5, 22),
        catchup=False,
        tags=['sixth_task'],
) as dag:
    task1 = BashOperator(
        task_id='pwd',
        bash_command="pwd",
    )

    for i in range(1, 6):
        task = BashOperator(
            task_id=f'echo_task{i}',
            bash_command="echo $NUMBER",
            env={'NUMBER': str(i)}
        )
        task1 >> task
