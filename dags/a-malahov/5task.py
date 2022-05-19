from datetime import datetime, timedelta
from textwrap import dedent
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.bash import BashOperator

with DAG(
        'a-malahov_task5',
        default_args={
            'depends_on_past': False,
            'email': ['airflow@example.com'],
            'email_on_failure': False,
            'email_on_retry': False,
            'retries': 1,
            'retry_delay': timedelta(minutes=5),  # timedelta из пакета datetime
        },
        description='a-malahov task 1',
        schedule_interval=timedelta(days=1),
        start_date=datetime(2022, 5, 10),
        catchup=False,
        tags=['malahov'],
) as dag:
    for i in range(10):
        bash_op = BashOperator(
            task_id=f'run_bash_number_{i}',
            bash_command=f'echo $NUMBER',
            env={"NUMBER": str(i)}
        )

    bash_op

