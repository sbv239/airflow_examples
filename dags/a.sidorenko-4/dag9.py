from datetime import datetime, timedelta
from textwrap import dedent

# Для объявления DAG нужно импортировать класс из airflow
from airflow import DAG
# PostgresHook

import psycopg2
from psycopg2.extras import RealDictCursor

from airflow.operators.python import PythonOperator

with DAG(
    'hw_10_a.sidorenko-4_connect',
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
    description='a.sidorov HW 10 Connections',
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
    tags=['task10'],
) as dag:
    

    def connect_postgres(**kwargs):
        from airflow.hooks.base import BaseHook

        creds = BaseHook.get_connection("startml_feed")

        with psycopg2.connect(
            f"postgresql://{creds.login}:{creds.password}"
            f"@{creds.host}:{creds.port}/{creds.schema}"
            ) as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cursor:
                cursor.execute("""
                SELECT user_id, 
                COUNT(*)
                FROM  feed_action
                WHERE action = 'like'
                GROUP BY user_id
                ORDER BY COUNT(*) DESC
                LIMIT 1
                """)
                results = cursor.fetchone()
                return results


    from_db = PythonOperator(
            task_id=f'return_user_from_db',
            python_callable=connect_postgres)

    
    from_db