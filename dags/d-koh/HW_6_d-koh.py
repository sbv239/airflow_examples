from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator

with DAG(
        'HW_6_d-koh',
        default_args={
            'depends_on_past': False,
            'email': ['airflow@example.com'],
            'email_on_failure': False,
            'email_on_retry': False,
            'retries': 1,
            'retry_delay': timedelta(minutes=5),
        },
        description='DAG from step 3',
        schedule_interval=timedelta(days=1),
        start_date=datetime(2023, 6, 11),
        catchup=False,
        tags=['kokh'],
) as dag:
    for i in range(10):
        task_bash = BashOperator(
            task_id='print_task' + str(i),
            bash_command=f"echo $NUMBER",
            env={'NUMBER': str(i)}
        )
