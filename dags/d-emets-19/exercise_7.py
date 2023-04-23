from datetime import datetime, timedelta
from textwrap import dedent

from airflow import DAG

from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator


def print_ds(ts, run_id, **kwargs):
    print(kwargs['task_number'])
    print(ts)
    print(run_id)
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
        task = PythonOperator(
            task_id=f"python_op_{i}",
            python_callable=print_ds,
            op_kwargs={'task_number': i},
        )
