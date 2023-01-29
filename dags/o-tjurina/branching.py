"""
Test documentation
"""
from datetime import datetime, timedelta

# Для объявления DAG нужно импортировать класс из airflow
from airflow import DAG
from airflow.models import Variable
from airflow.operators.python import BranchPythonOperator
from airflow.operators.dummy import DummyOperator

with DAG(
        dag_id='hw_13_o-tjurina',
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
        description='A lesson11 task 13 DAG',
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
    def choose_branch():
        task_id = "not_startml_desc"
        is_startml = Variable.get("is_startml")
        if is_startml == "True":
            task_id = "startml_desc"
        return task_id

    def print_context(str):
        print(str)

    start = DummyOperator(
        task_id='before_branching',
        dag=dag,
    )

    branching = BranchPythonOperator(
        task_id='determine_course',
        python_callable=choose_branch,
    )

    task1 = BranchPythonOperator(
        task_id="startml_desc",
        python_callable=print_context,
        op_kwargs={'str': "StartML is a starter course for ambitious people"},
    )

    task2 = BranchPythonOperator(
        task_id="not_startml_desc",
        python_callable=print_context,
        op_kwargs={'str': "Not a startML course, sorry"},
    )


    end = DummyOperator(
        task_id='after_branching',
        dag=dag,
    )

    start >> branching >> [task1, task2] >> end
