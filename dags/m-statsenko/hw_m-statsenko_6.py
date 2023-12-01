from datetime import datetime, timedelta
from textwrap import dedent
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

default_args = {
    'depends_on_past': False,
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
        'hw_6_m-statsenko',
        default_args=default_args,
        description='HW 6',
        start_date=datetime(2023, 11, 30),
        catchup=False,
        tags=['HW 6']
) as dag:
    for i in range(10):
        task_b = BashOperator(
            task_id='number_' + str(i),
            bash_command="echo $NUMBER",
            env={'NUMBER': str(i)}
        )
