from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
with DAG(
    'dag_task_6',
    default_args={
        'depends_on_past': False,
        'email': ['airflow@example.com'],
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 1,
        'retry_delay': timedelta(minutes=5),  # timedelta из пакета datetime
        },
    start_date = datetime(2023, 3, 26)
) as dag:
    for i in range(10):
        NUMBER = i,
        t1 = BashOperator(
            task_id = str(i),
            bash_command = "echo $NUMBER",
            env = {"NUMBER": str(i)}
        )
