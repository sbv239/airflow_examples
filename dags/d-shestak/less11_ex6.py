from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from textwrap import dedent

default_args = {
    'depends_on_past': False,
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),

}
with DAG('hw_d-shestak_6',
         default_args=default_args,
         description='hw_d-shestak_6',
         schedule_interval=timedelta(days=1),
         start_date=datetime(2023, 10, 21),
         tags=['hw_6_d-shestak']
         ) as dag:
    for i in range(10):
        BashOperator(
            task_id=f'task_{i}_BashOperator',
            bash_command="echo $NUMBER",
            env={'NUMBER': i}
        )