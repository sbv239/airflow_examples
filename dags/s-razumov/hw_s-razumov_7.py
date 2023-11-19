from datetime import datetime, timedelta

from airflow import DAG

from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator


def print_number_task(ts, run_id, **kwargs):
    print(f"{ts}")
    print(f"{run_id}")


with DAG(
        'hw_s-razumov_7',
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

        start_date=datetime(2023, 1, 1),
        tags=['hw_s-razumov_7'],
) as dag:

    for task_number in range(10, 30):
        t2 = PythonOperator(
            task_id='print_task_' + str(task_number),
            python_callable=print_number_task,
            op_kwargs={'task_number': task_number},
            dag=dag,
        )
