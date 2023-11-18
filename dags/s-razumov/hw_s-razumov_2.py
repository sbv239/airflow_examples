from datetime import datetime, timedelta

from airflow import DAG


from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

def print_ds(ds, **kwargs):
    print(ds)
    return 'My first PythonOperator'

with DAG(
    'hw_2_s-razumov',
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

    start_date=datetime(202, 1, 1),
    tags=['hw_s-razumov-2'],
) as dag:

    t1 = BashOperator(
        task_id='present_working_directory',
        bash_command='pwd',
    )

    t2 = PythonOperator(
        task_id='print_date',
        python_callable=print_ds,
    )

    t1 >> t2
