from airflow import DAG
from datetime import datetime, timedelta
from textwrap import dedent
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

with DAG(
    'hw_vl_nikolajchuk_6',
    default_args={
        'depends_on_past': False,
        'email': ['airflow@example.com'],
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 1,
        'retry_delay': timedelta(minutes=5),
    },
    description='hw',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2023, 11, 27),
    catchup=False,
    tags=["hw_6"]
) as dag:

    for i in range(10):
        t1 = BashOperator(
            task_id=f"task_bash_{i}",
            bash_command=f"echo $NUMBER",
            env={'NUMBER': str(i)}
        )