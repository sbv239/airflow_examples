"""
Test documentation
"""
from datetime import datetime, timedelta
from textwrap import dedent

from airflow import DAG


from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
with DAG(
    'hw_5_a-haletina-7',

    default_args={
        'depends_on_past': False,
        'email': ['airflow@example.com'],
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 1,
        'retry_delay': timedelta(minutes=5),
    },
    description='hw_5_a-haletina-7',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2022, 1, 1),
    catchup=False,
    tags=['hw_5_a-haletina-7'],
) as dag:
    
    for i in range(1, 11):
        t1 = BashOperator(
            env={"NUMBER": i},
            task_id='task_number' + str(i),
            bash_command= "echo $NUMBER",
        )
    t1
