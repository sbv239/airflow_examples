from datetime import datetime, timedelta
from textwrap import dedent

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python_operator import PythonOperator

with DAG('hw_9_o-beljakov-7',
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
    description='task_9',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2022, 5, 8),
    catchup=False,
    tags=['task_9'], ) as dag:

    
    def give_str():
        return "Airflow tracks everything"

    def get_str(ti):
        testing_increases = ti.xcom_pull(
            key='return_value',
            task_ids='give_str'
            )
        print(testing_increases)
    
    # Каждый таск будет спать некое количество секунд
    task = PythonOperator(
            task_id='give_str',  # в id можно делать все, что разрешают строки в python
            python_callable = give_str,
            # передаем в аргумент с названием random_base значение float(i) / 10
            )
    # настраиваем зависимости между задачами
    # run_this - это некий таск, объявленный ранее (в этом примере не объявлен)

    task2 = PythonOperator(
            task_id='get_str',  # в id можно делать все, что разрешают строки в python
            python_callable = get_str,
            # передаем в аргумент с названием random_base значение float(i) / 10
            )
    task>>task2