from datetime import datetime, timedelta
from textwrap import dedent

from airflow import DAG

from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator


def get_sql():
    from airflow.providers.postgres.operators.postgres import PostgresHook
    from psycopg2.extras import RealDictCursor

    postgres = PostgresHook(postgres_conn_id="startml_feed")
    with postgres.get_conn() as conn:  # вернет тот же connection, что вернул бы psycopg2.connect(...)
        with conn.cursor(cursor_factory=RealDictCursor) as cursor:
            cursor.execute("""
            SELECT user_id, count(*) as count
            FROM feed_action
            WHERE action = 'like'
            GROUP BY user_id
            ORDER BY count DESC
            LIMIT 1
        """
                           )
            return cursor.fetchall()


with DAG(
        'hw_10_n-shishmakova-7',
        # Параметры по умолчанию для тасок
        default_args={
            'depends_on_past': False,
            'email': ['airflow@example.com'],
            'email_on_failure': False,
            'email_on_retry': False,
            'retries': 1,
            'retry_delay': timedelta(minutes=5),  # timedelta из пакета datetime
        },
        # Описание DAG (не тасок, а самого DAG)
        description='DAG for 10 task in lesson 11',
        # Как часто запускать DAG
        schedule_interval=timedelta(days=1),
        start_date=datetime(2022, 5, 1),
        # Запустить за старые даты относительно сегодня
        catchup=False,
        # теги, способ помечать даги
        tags=['hw10'],
) as dag:
    t1 = PythonOperator(
        task_id='sql',  # нужен task_id, как и всем операторам
        python_callable=get_sql
    )
