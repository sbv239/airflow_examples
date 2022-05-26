from airflow import DAG

from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from textwrap import dedent

default_args = {
    'depends_on_past': False,
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

"""
first DAG creation for HW_1
"""

def print_ds(ds, **kwargs):
    """Function for PythonOperator"""
    print(ds)
    print(kwargs)
    return 'helo my first dag'

with DAG(
    'first_DAG',
    default_args=default_args,
    description='A simple first DAG',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2022, 5, 25),
    catchup=False,
    tags=['first']) as dag:
    t1 = BashOperator(
        task_id = 'show_pwd',
        bash_command = 'pwd'
    )

    def print_ds(ds):
        """Function for PythonOperator"""
        print(ds)
        return 'helo my first dag'

    t2 = PythonOperator (
        task_id = 'print ds',
        python_callable = print_ds
    )

    t1.doc_md = dedent(
            """
            show the directory where your Airflow code is running.
            """
    )
    t2.doc_md - dedent(
            """
            take the ds argument and print it.
            """
    )

    dag.doc_md = __doc__

    t1 >> t2