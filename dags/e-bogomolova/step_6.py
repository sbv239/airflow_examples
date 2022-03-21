import os
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator

with DAG(
    'e_bogomolova_step_6',
    default_args={
        'depends_on_past': False,
        'email': ['airflow@example.com'],
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 1,
        'retry_delay': timedelta(minutes=5),
    },
    description='DAG_for_e_bogomolova_step_6',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2022, 3, 18),
    catchup=False,
    tags=['step_6'],
) as dag:

    for i in range(10):
        os.environ['NUMBER'] = str(i)
        t1 = BashOperator(
            task_id='echo_' + str(i),
            bash_command='echo $NUMBER',
        )

    t1
