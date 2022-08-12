from datetime import timedelta, datetime
from airflow.operators.bash import BashOperator
from textwrap import dedent

from airflow import DAG

with DAG(
    'hw_2_de-jakovlev',
    default_args={
        'depends_on_past': False,
        'email': ['airflow@example.com'],
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 1,
        'retry_delay': timedelta(minutes=5),  # timedelta из пакета datetime
    },
    description='A simple tutorial DAG',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2022, 1, 1),
    catchup=False,
    tags=['example'],
) as dag:
    t1 = BashOperator(
        task_id='print_date',
        bash_command='date'
    )
    t2 = BashOperator(
        task_id='sleep',
        depends_on_past=False,
        bash_command='sleep 5',
        retries=3,
    )
    t1.doc_md = dedent (
        """
        Example
        """
    )
    t1 >> t2




