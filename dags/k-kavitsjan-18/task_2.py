from datetime import datetime, timedelta
from textwrap import dedent

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

def print_ds(ds):
    print(ds)


with DAG(
    'k-kavitsjan-18_task_2',
    default_args={
        'depends_on_past': False,
        'email': ['airflow@example.com'],
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 1,
        'retry_delay': timedelta(minutes=5),
        },
    description='task_2',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2023, 3, 20),
    catchup=False,
    tags=['k-kavitsjan-18'],
    ) as dag:
    t1 = BashOperator(
        task_id='print_pwd',
        bash_command='pwd',
    )

    t2 = PythonOperator(
        task_id='print_ds',
        python_callable=print_ds,
    )
    dag.doc_md="""
    This is documentation"""

    t1 >> t2
