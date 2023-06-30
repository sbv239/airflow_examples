from airflow import DAG
from airflow.operators.bash import BashOperator

from datetime import timedelta, datetime

with DAG(
    'hw_e.mironenko-13_6',
    default_args={
        'depends_on_past': False,
        'email': ['airflow@example.com'],
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 1,
        'retry_delay': timedelta(minutes=5),  # timedelta из пакета datetime
            },
    start_date=datetime(2023, 6, 26),
    schedule_interval=timedelta(days=1),
    catchup=False,
    tags = ['e.mironenko-13']
) as dag:

    for i in range(1, 11):
        t1 = BashOperator(
            task_id=f"task_id_{i})",
            bash_command="echo $NUMBER",
            env={"NUMBER": i}
        )
