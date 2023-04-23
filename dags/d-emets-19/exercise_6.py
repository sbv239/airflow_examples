from datetime import datetime, timedelta
from textwrap import dedent

from airflow import DAG

from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator


def print_ds(ds, **kwargs):
    print(kwargs['i'])
    return 'Some logs'


with DAG(
        'e_6_demets',
        # Параметры по умолчанию для тасок
        default_args={
            'depends_on_past': False,
            'email': ['airflow@example.com'],
            'email_on_failure': False,
            'email_on_retry': False,
            'retries': 1,
            'retry_delay': timedelta(minutes=5),  # timedelta из пакета datetime
        },
        description='A simple tutorial DAG',
        schedule_interval=timedelta(days=1),
        start_date=datetime(2023, 4, 23),
        catchup=False,
        tags=['demets'],
) as dag:
    for i in range(30):
        task = BashOperator(
            task_id=f"bash_op_{i}",
            bash_command="echo $NUMBER",
            env={"NUMBER": str(i)}
        )
