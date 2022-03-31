from airflow import DAG
from datetime import timedelta, datetime

from airflow.operators.bash import BashOperator

with DAG(
    'rakhimova_task5',
    # Параметры по умолчанию для тасок
    default_args={
        'depends_on_past': False,
        'email': ['airflow@example.com'],
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 1,
        'retry_delay': timedelta(minutes=5),  # timedelta из пакета datetime

    },
    description='DAG5 Rakhimova',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2022, 3, 25),
    catchup=False,
    tags=['hehe'],
) as dag:

    for i in range(10):
        t_bash = BashOperator(
            task_id=f'bash_task_{i}',
            bash_command='echo $NUMBER',
            env={'NUMBER': i}
        )
