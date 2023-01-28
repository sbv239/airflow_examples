"""
Test documentation
"""
from datetime import datetime, timedelta

# Для объявления DAG нужно импортировать класс из airflow
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.models import Variable

with DAG(
        dag_id='hw_12_o-tjurina',
        # Параметры по умолчанию для тасок
        default_args={
            # Если прошлые запуски упали, надо ли ждать их успеха
            'depends_on_past': False,
            # Кому писать при провале
            'email': ['olesia.tiurina@outlook.com'],
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
        description='A lesson11 task 12 DAG',
        # Как часто запускать DAG
        schedule_interval=timedelta(days=1),
        # С какой даты начать запускать DAG
        # Каждый DAG "видит" свою "дату запуска"
        # это когда он предположительно должен был
        # запуститься. Не всегда совпадает с датой на вашем компьютере
        start_date=datetime(2023, 1, 27),
        # Запустить за старые даты относительно сегодня
        # https://airflow.apache.org/docs/apache-airflow/stable/dag-run.html
        catchup=False,
        # теги, способ помечать даги
        tags=['example'],
) as dag:
    def print_context():
        # необходимо передать имя, заданное при создании Variable
        is_startml = Variable.get("is_startml")
        # теперь в is_prod лежит значение Variable
        print(is_startml)
        return is_startml


    get_var = PythonOperator(
        # provide context is for getting the TI (task instance ) parameters
        task_id='get_user_with_max_likes',
        python_callable=print_context,
    )

    # А вот так в Airflow указывается последовательность задач
    get_var
