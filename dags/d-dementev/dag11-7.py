"""
my documentation
"""

from datetime import datetime, timedelta
from textwrap import dedent

from airflow import DAG

from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator




with DAG(
    'unit11-7-dementev',
        default_args={
            'depends_on_past': False,
            'email': ['airflow@example.com'],
            'email_on_failure': False,
            'email_on_retry': False,
            'retries': 1,
            'retry_delay': timedelta(minutes=5),  # timedelta из пакета datetime
        },




    description='DAG unit 11-2',
        schedule_interval=timedelta(days=1),
        start_date=datetime(2023, 1, 1),
        catchup=False,



) as dag:
    def print_smth(ts, run_id, **kwargs):
        print(ts, run_id, kwargs[task_number])
        return None

    for i in range(10):
        t1 = BashOperator(
            task_id=f'print_{i}',
            bash_command=f"echo {i}",
        )
    for i in range(20):
        t2 = PythonOperator(
            task_id=f'print_smth_{i}',
            python_callable=print_smth,
            op_kwargs={'task_number': i}
        )
    t1 >> t2







