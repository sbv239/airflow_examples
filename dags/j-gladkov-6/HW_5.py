from airflow import DAG

from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

from datetime import datetime, timedelta
from textwrap import dedent

with DAG(
    'hw_5_j-gladkov-6',
    default_args={
        'depends_on_past': False,
        'email': ['airflow@example.com'],
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 1,
        'retry_delay': timedelta(minutes=5),
    },
    description = "Env в BashOperator",
    schedule_interval = timedelta(days=1),
    start_date = datetime(2022, 3, 22),
    catchup = False,
    tags = ['hw_5'],
) as dag:



    for i in range(1, 11):

        task_bash = BashOperator(
            task_id = "echo_" + str(i),
            bash_command = "echo $NUMBER",
            env={"NUMBER": str(i)}
        )
