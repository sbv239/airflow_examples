from datetime import datetime, timedelta
from textwrap import dedent

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator


with DAG(
    'a-petuhova_step6',
    default_args={
        'depends_on_past': False,
        'email': ['airflow@example.com'],
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 1,
        'retry_delay': timedelta(minutes=5),  # timedelta из пакета datetime
    },
    description='Task for step 6 a-petuhova',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2023, 5, 23),
    catchup=False,
    tags=['a-petuhova'],
) as dag:

    for NUMBER in range(10):
        task = BashOperator(
            task_id=f'task_bash_{NUMBER}',
            bash_command="echo $NUMBER",
            env = {'NUMBER': NUMBER}
        )
        task >> task