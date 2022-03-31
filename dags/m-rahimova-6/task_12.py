"""

BranchingOperator - это оператор, который по некоторому условию определяет, в какое ответвление пойдет выполнение DAG.
Один из способов определить это "некоторое условие" - это задать python функцию, которая будет возвращать task_id,
куда надо перейти после ветвления.

Создайте DAG, имеющий BranchPythonOperator. Логика ветвления должна быть следующая: если значение Variable is_startml
равно True, то перейти в таску с task_id="startml_desc", иначе перейти в таску с task_id="not_startml_desc".

Затем объявите две задачи с task_id="startml_desc" и task_id="not_startml_desc".

В первой таске распечатайте "StartML is a starter course for ambitious people", во второй "Not a startML course, sorry".

Перед BranchPythonOperator можете поставить DummyOperator - он ничего не делает, но зато задает красивую
"стартовую точку" на графе. Точно так же можете поставить DummyOperator в конце DAG.

"""

from airflow import DAG
from datetime import datetime, timedelta

from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.dummy import DummyOperator


def branching():
    from airflow.models import Variable

    is_startml = Variable.get('is_startml')
    if is_startml:
        return 'startml_desc'
    else:
        return 'not_startml_desc'


def startml_desc():
    print("StartML is a starter course for ambitious people")


def not_startml_desc():
    print("Not a startML course, sorry")


with DAG(
        'rakhimova_task12',
        # Параметры по умолчанию для тасок
        default_args={
            'depends_on_past': False,
            'email': ['airflow@example.com'],
            'email_on_failure': False,
            'email_on_retry': False,
            'retries': 1,
            'retry_delay': timedelta(minutes=5),  # timedelta из пакета datetime

        },
        description='DAG12 Rakhimova',
        schedule_interval=timedelta(days=1),
        start_date=datetime(2022, 3, 25),
        catchup=False,
        tags=['hehe'],
) as dag:

    starting = DummyOperator(
        task_id='start'
    )

    br = BranchPythonOperator(
        task_id='branching',
        python_callable=branching,
    )

    st_ml = PythonOperator(
        task_id='startml_desc',
        python_callable=startml_desc,
    )

    not_st_ml = PythonOperator(
        task_id='not_startml_desc',
        python_callable=not_startml_desc,
    )

    ending = DummyOperator(
        task_id='stop'
    )

    starting >> br >> [st_ml, not_st_ml] >> ending
