from airflow.operators.bash import BashOperator
from airflow import DAG
from datetime import datetime, timedelta


with DAG(
    'hw_6_i-vekhov',
    default_args={
        'depends_on_past': False,
        'email': ['airflow@example.com'],
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 1,
        'retry_delay': timedelta(minutes=5),  # timedelta из пакета datetime
    },
    description='hw_6_i-vekhov',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2021, 3, 29),
    catchup=False,
    tags=['hw_6_i-vekhov'],
) as dag:

    for i in range(10):
        NUMBER = i
        task_bash = BashOperator(
                task_id=f'task_bash_{i}',
                bash_command='echo $NUMBER',
                env={'NUMBER': str(i)}
        )

        task_bash
