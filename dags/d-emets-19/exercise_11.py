from datetime import datetime, timedelta

from airflow import DAG

from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from psycopg2.extras import RealDictCursor


def likes_handler():
    postgres = PostgresHook(postgres_conn_id='startml_feed')
    with postgres.get_conn() as conn:  # вернет тот же connection, что вернул бы psycopg2.connect(...)
        with conn.cursor(cursor_factory=RealDictCursor) as cursor:
            cursor.execute("""
                SELECT
                user_id,
                COUNT(*) as count
                FROM feed_action
                WHERE action='like'
                GROUP BY user_id
                ORDER BY COUNT(*) DESC
                LIMIT 1
            """)
            return cursor.fetchone()

with DAG(
        'e_11_demets',
        # Параметры по умолчанию для тасок
        default_args={
            'depends_on_past': False,
            'email': ['airflow@example.com'],
            'email_on_failure': False,
            'email_on_retry': False,
            'retries': 1,
            'retry_delay': timedelta(minutes=5),  # timedelta из пакета datetime
        },
        description='A simple tutorial DAG',
        schedule_interval=timedelta(days=1),
        start_date=datetime(2022, 1, 1),
        catchup=False,
        tags=['demets'],
) as dag:

    t1 = PythonOperator(
        task_id='first',
        python_callable=likes_handler)
