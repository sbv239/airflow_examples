from datetime import datetime, timedelta

from airflow import DAG

from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator


def print_ds(ds, **kwargs):
    print(kwargs['i'])
    return 'Some logs'


with DAG(
        'e_3_demets',
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
        tags=['exercise 3'],
) as dag:
    for i in range(30):
        if i < 10:
            BashOperator(
                task_id=f"bash_op_{i}",
                bash_command=f"echo {i}",
            )
        else:
            PythonOperator(
                task_id=f"python_op_{i}",
                python_callable=print_ds,
                op_kwargs={'i': i}
            )
