"""
DAG c BashOperator, PythonOperator
"""
from datetime import datetime, timedelta
from textwrap import dedent

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python_operator import PythonOperator

with DAG(
    'hw_3_a-kalinkin',
    # Параметры по умолчанию для тасок
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

    # Описание DAG (не тасок, а самого DAG)
    description='First DAG',
    # Как часто запускать DAG
    schedule_interval=timedelta(days=1),
    # С какой даты начать запускать DAG
    # Каждый DAG "видит" свою "дату запуска"
    # это когда он предположительно должен был
    # запуститься. Не всегда совпадает с датой на вашем компьютере
    start_date=datetime(2022, 1, 1),
    # Запустить за старые даты относительно сегодня
    # https://airflow.apache.org/docs/apache-airflow/stable/dag-run.html
    catchup=False,
    # теги, способ помечать даги
    tags=['hw_2_tag_a-kalinkin'],
) as dag:

    def print_task_number(op_kwargs):
        print(f"task number is: {op_kwargs}")

    for i in range(30):
        if i<10:
            t=BashOperator(
                task_id='print_number_of_loop',
                depends_on_past=False,
                bash_command=f"echo {i}"
            )
        else:
            t1=PythonOperator(
                task_id='print_task_number',  # нужен task_id, как и всем операторам
                python_callable=print_task_number

            )

    t >> t1