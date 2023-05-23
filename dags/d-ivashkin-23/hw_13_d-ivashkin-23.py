from airflow.operators.python import BranchPythonOperator, PythonOperator
from airflow.operators.dummy import DummyOperator
from airflow import DAG
from datetime import datetime, timedelta


"""
BranchingOperator - это оператор, который по некоторому условию определяет, в какое ответвление пойдет выполнение DAG. 
Один из способов определить это "некоторое условие" - это задать python функцию, которая будет возвращать task_id, 
куда надо перейти после ветвления.

Создайте DAG, имеющий BranchPythonOperator. Логика ветвления должна быть следующая: если значение Variable is_startml 
равно "True", то перейти в таску с task_id="startml_desc", иначе перейти в таску с task_id="not_startml_desc". 
Затем объявите две задачи с task_id="startml_desc" и task_id="not_startml_desc".

NB: класс Variable возвращает строку!

В первой таске распечатайте "StartML is a starter course for ambitious people", во второй "Not a startML course, sorry".

Перед BranchPythonOperator можете поставить DummyOperator - он ничего не делает, но зато задает красивую 
"стартовую точку" на графе. Точно так же можете поставить DummyOperator в конце DAG.
"""

with DAG(
    'hw_d-ivashkin-23_13',
    default_args={
        'depends_on_past': False,
        'email': ['airflow@example.com'],
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 1,
        'retry_delay': timedelta(minutes=5),
    },
    description='Homework 13-th step DAG',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2023, 5, 21),
    catchup=False,
    tags=['homework', 'di']
) as dag:

    def startml():
        print("StartML is a starter course for ambitious people")

    def not_startml():
        print("Not a startML course, sorry")

    def get_var():
        from airflow.models import Variable
        is_startml = Variable.get('is_startml')
        return is_startml

    def is_startml_func(**kwargs):
        v = get_var()
        if v == 'True':
            return 'startml_desc'
        else:
            return 'not_startml_desc'


    t1 = DummyOperator(
        task_id='before_branching'
    )

    t2 = BranchPythonOperator(
        task_id='determine_course',
        python_callable=is_startml_func
    )

    t3 = PythonOperator(
        task_id='startml_desc',
        python_callable=startml
    )

    t4 = PythonOperator(
        task_id='not_startml_desc',
        python_callable=not_startml
    )

    t5 = DummyOperator(
        task_id='after_branching'
    )

t1 >> t2 >> [t3, t4] >> t5
