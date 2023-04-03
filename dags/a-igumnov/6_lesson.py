from datetime import datetime, timedelta
from textwrap import dedent

from airflow import DAG

from airflow.operators.bash import BashOperator
with DAG(
    'a-igumnov_task_6',

    default_args={
    'depends_on_past': False,
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),  # timedelta из пакета datetime
    },
    description = 'hw_6_a-igumnov',
    schedule_interval = timedelta(days=1),
    start_date=datetime(2023, 1, 1),
    catchup=False,
    tags=['hw_6_a-igumnov']


) as dag:

    for i in range(1, 11):
        NUMBER = i
        t1 = BashOperator(
            task_id = f'hw_2_{i}',
            bash_command = 'echo $NUMBER',
            env = {'NUMBER' : str(i)}
        )


    t1