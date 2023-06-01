from datetime import datetime, timedelta
from textwrap import dedent

from airflow import DAG

from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

with DAG(
        'hw_dmi-rusin_6',
        default_args={
            'depends_on_past': False,
            'email': ['airflow@example.com'],
            'email_on_failure': False,
            'email_on_retry': False,
            'retries': 1,
            'retry_delay': timedelta(minutes=5),  # timedelta из пакета datetime
        },
        description='Second task',
        schedule_interval=timedelta(minutes=5),
        start_date=datetime(2023, 6, 1),
        catchup=False,
        tags=['example'],
) as dag:
    for i in range(10):
        t1 = BashOperator(
            task_id='print_' + str(i),
            bash_command="echo $NUMBER",
            env={"NUMBER": i}
        )

