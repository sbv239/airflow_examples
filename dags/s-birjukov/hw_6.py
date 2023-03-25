from datetime import datetime, timedelta
from textwrap import dedent

from airflow import DAG
from airflow.operators.bash import BashOperator

with DAG(
    'hw_6_s-birjukov',    # название DAG
    # ниже идут параметры по умолчанию
    default_args={
        'depends_on_past': False,
        'email': ['airflow@example.com'],
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 1,
        'retry_delay': timedelta(minutes=5),  # timedelta из пакета datetime
    },
    # описание DAG
    description='hw_6_s-birjukov',
    start_date=datetime(2022, 1, 1),
) as dag:

    for task_number in range(10):
        t1 = BashOperator(
            task_id=f'env_bash_operator_{task_number}',
            bash_command=f"echo $NUMBER",
            env = {"NUMBER" : task_number}
        )