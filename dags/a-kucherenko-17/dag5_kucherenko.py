from datetime import datetime, timedelta
from textwrap import dedent

# Для объявления DAG нужно импортировать класс из airflow
from airflow import DAG
from airflow.operators.bash import BashOperator

with DAG(
        'kucherenko-hw5',
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
        description='a-kucherenko-17-dag-5',
        # Как часто запускать DAG
        schedule_interval=timedelta(days=1),
        # С какой даты начать запускать DAG
        # Каждый DAG "видит" свою "дату запуска"
        # это когда он предположительно должен был
        # запуститься. Не всегда совпадает с датой на вашем компьютере
        start_date=datetime(2023, 2, 21),
        # Запустить за старые даты относительно сегодня
        # https://airflow.apache.org/docs/apache-airflow/stable/dag-run.html
        catchup=False,
        # теги, способ помечать даги
        tags=['kucherenko-hw5'],
) as dag:

    logical_date = "{{ ts }}"
    id_of_the_run = "{{ run_id }}"

    for i in range(5):
        task1 = BashOperator(
            task_id='print_ts_run_id_' + str(i),
            bash_command=f"echo {logical_date} and {id_of_the_run} i",
        )
