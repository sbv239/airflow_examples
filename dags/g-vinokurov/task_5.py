from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
from textwrap import dedent

with DAG(
    'hw_g-vinokurov_5',
    default_args={
        'depends_on_past': False,
        'email': ['airflow@example.com'],
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 1,
        'retry_delay': timedelta(minutes=5),
    },
        description='DAG in task_4',
        schedule_interval=timedelta(days=1),
        start_date=datetime(2023, 9, 28),
        catchup=False,
) as dag:

    for i in range(10):
        NUMBER = i
        operator_1 = BashOperator(
            task_id=f"echo_" + str(i),
            bash_command="echo $NUMBER ",
            env={"NUMBER": str(i)},
        )