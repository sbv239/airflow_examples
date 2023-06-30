from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta

default_args = {'depends_on_past': False,
                'email': ['airflow@example.com'],
                'email_on_failure': False,
                'email_on_retry': False,
                'retries': 1,
                'retry_delay': timedelta(minutes=5),
                }

with DAG(
    'hw_j-kutnjak-21_6',
    default_args=default_args,
    description='A simple tutorial DAG',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2023, 6, 30),
    catchup=False,
    tags=['example'],
) as dag:

    for i in range(10):
        NUMBER = i
        t1 = BashOperator(
            task_id='print_number_id_' + str(i),
            bash_command="echo $NUMBER",
            env={"NUMBER": str(i)}
        )
