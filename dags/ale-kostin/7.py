from datetime import datetime, timedelta
from textwrap import dedent

from airflow import DAG

from airflow.operators.bash import BashOperator
from airflow.operators.python_operator import PythonOperator


def print_fnc(ts, run_id, **kwargs):
    print(ts, run_id, f'{kwargs["task_number"]}')


with DAG(
        'tutorial',
        default_args={
            # Если прошлые запуски упали, надо ли ждать их успеха
            'depends_on_past': False,
            # Кому писать при провале
            'email': ['airflow@example.com'],
            # А писать ли вообще при провале?
            'email_on_failure': False,
            # Писать ли при автоматическом перезапуске по провалу
            'email_on_retry': False,
            # Сколько раз пытаться запустить, далее помечать как failed
            'retries': 1,
            # Сколько ждать между перезапусками
            'retry_delay': timedelta(minutes=5),  # timedelta из пакета datetime
        },
        description='A simple tutorial DAG',
        schedule_interval=timedelta(days=1),
        start_date=datetime(2022, 1, 1),
        catchup=False
) as dag:
    for i in range(10, 30):
        python_task = PythonOperator(
            task_id=f'python_operator{i}',
            python_callable=print_fnc,
            op_kwargs={'task_number': i}
        )

