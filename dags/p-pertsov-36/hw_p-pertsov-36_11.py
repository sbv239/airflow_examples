from datetime import datetime, timedelta
from textwrap import dedent
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.hooks.base import BaseHook
import psycopg2

def top_likes_user():
    postgres = PostgresHook(postgres_conn_id="startml_feed")
    with postgres.get_conn() as conn:   # вернет тот же connection, что вернул бы psycopg2.connect(...)
        with conn.cursor() as cursor:
            cursor.execute("""
                SELECT fa.user_id, COUNT(fa.action)
                FROM "feed_action" fa
                WHERE fa.action = 'like'
                GROUP BY fa.user_id
                ORDER BY COUNT(fa.action) DESC
                LIMIT 1
            """)
            result = cursor.fetchone()
            return result

with DAG(
    'hw_p-pertsov-36_11',
    default_args={
        'depends_on_past': False,
        'email': ['airflow@example.com'],
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 1,
        'retry_delay': timedelta(minutes=5),
    },
    description='DAG for p-pertsov-36 task 11',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2023, 6, 22),
    catchup=False,
    tags=['pavelp_hw_11'],
) as dag:
    t1 = PythonOperator(
        task_id='find_user_task',
        python_callable=top_likes_user,
        provide_context=True
    )

t1