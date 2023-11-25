from datetime import datetime, timedelta
from textwrap import dedent
from airflow import DAG

from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

with DAG(
    'hw_kamilahmadov_6',
    default_args={
        'depends_on_past': False,
        'email': ['airflow@example.com'],
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 1,
        'retry_delay': timedelta(minutes=5),
    },
    description='first DAG',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2023, 11, 20),
    catchup=False,
    tags=["hw_6"]
) as dag:

    for i in range(10):
        t1 = BashOperator(
            task_id=f"random_bash_{i}",
            bash_command=f"echo $NUMBER",
            env={'NUMBER': str(i)}
        )