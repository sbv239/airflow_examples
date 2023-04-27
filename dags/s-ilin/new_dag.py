"""
Test documentation
"""
from datetime import datetime, timedelta
from textwrap import dedent

from airflow import DAG

from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator


with DAG(
    'first_dag',
        default_args={
            'depends_on_past': False,
            'email': ['airflow@example.com'],
            'email_on_failure': False,
            'email_on_retry': False,
            'retries': 1,
            'retry_delay': timedelta(minutes=5),
        },
    description='A simple tutorial DAG',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2023, 4, 26),
    catchup=False,
    tags=['example'],
) as dag:
    for i in range(5):
        s_ilin_1 = BashOperator(
            task_id=f"ilin_curdir_{i}",
            bash_command=f'echo {{ ts }} & echo {{ run_id }}',
        )
        s_ilin_1.doc_md = dedent(
            """#### Doc `i`
            **yes** *yes*
            """
        )

s_ilin_1
