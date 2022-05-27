from datetime import datetime, timedelta
from textwrap import dedent

from airflow import DAG

from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

with DAG(
        'gul_assignment_1',
        default_args={
            'depends_on_past': False,
            'email': ['airflow@example.com'],
            'email_on_failure': False,
            'email_on_retry': False,
            'retries': 1,
            'retry_delay': timedelta(minutes=5)
        },
        description='Assignment 1 DAG',
) as dag:
    t1 = BashOperator(
        task_id='Execute pwd',
        bash_command='pwd'
    )


    def print_logical_date(ds):
        print(ds)
        print('Success')
        return "If you see this the task was successful"


    t2 = PythonOperator(
        task_id='Get logical date',
        python_callable = print_logical_date
    )
