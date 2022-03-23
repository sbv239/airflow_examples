from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
import os

with DAG(
        'n-anufrieva_task6',
        default_args={
            'depends_on_past': False,
            'email': ['airflow@example.com'],
            'email_on_failure': False,
            'email_on_retry': False,
            'retries': 1,
            'retry_delay': timedelta(minutes=5),
        },
        description='DAG n-anufrieva_task6',
        schedule_interval=timedelta(days=1),
        start_date=datetime(2022, 3, 22),
        catchup=False,
        tags=['n-anufrieva_task6'],
) as dag:
    for i in range(10):
        os.environ['NUMBER'] = str(i)
        task_bash = BashOperator(
            task_id='bash_task_' + str(i),
            bash_command="echo $NUMBER",
        )
    task_bash
