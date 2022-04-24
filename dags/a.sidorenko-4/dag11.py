from datetime import datetime, timedelta
from textwrap import dedent

# Для объявления DAG нужно импортировать класс из airflow
from airflow import DAG
# PostgresHook

import psycopg2
from psycopg2.extras import RealDictCursor

from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.dummy import DummyOperator


with DAG(
    'hw_12_a.sidorenko-4_Variables',
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
    # # Описание DAG (не тасок, а самого DAG)
    description='a.sidorov HW 12 Variables',
    # # Как часто запускать DAG
    schedule_interval=timedelta(days=1),
    # # С какой даты начать запускать DAG
    # # Каждый DAG "видит" свою "дату запуска"
    # # это когда он предположительно должен был
    # # запуститься. Не всегда совпадает с датой на вашем компьютере
    start_date=datetime(2022, 4, 23),
    # # Запустить за старые даты относительно сегодня
    # # https://airflow.apache.org/docs/apache-airflow/stable/dag-run.html
    catchup=False,
    # # теги, способ помечать даги
    tags=['task12'],
) as dag:
    
    def branching():
        from airflow.models import Variable

        if Variable.get("is_startml") == 'True':
            return "startml_desc"
        return "not_startml_desc"
    
    def start_ml_first():
        print('StartML is a starter course for ambitious people') 

    def isnot_start_ml():
        print('Not a startML course, sorry') 
    

    startml_desc = PythonOperator(
            task_id='startml_desc',
            python_callable=start_ml_first)


    not_startml_desc = PythonOperator(  
            task_id='not_startml_desc',
            python_callable=isnot_start_ml)

    checkTask = BranchPythonOperator(
        task_id='determine_course',
        python_callable=branching
        )

    started = DummyOperator(task_id='before_branching')
    finish = DummyOperator(task_id='after_branching')

    started >> checkTask >> [startml_desc, not_startml_desc] >> finish