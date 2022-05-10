from datetime import datetime, timedelta
from textwrap import dedent
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta
from airflow.hooks.base import BaseHook
from psycopg2.extras import RealDictCursor
import psycopg2
from airflow.providers.postgres.operators.postgres import PostgresHook

postgres = PostgresHook(postgres_conn_id='startml_feed')


def task_1():
    with postgres.get_conn() as conn:
        with conn.cursor(cursor_factory=RealDictCursor) as cursor:
            cursor.execute("""
                SELECT f.user_id, COUNT(f.user_id)
                FROM feed_action f
                WHERE f.action = 'like'
                GROUP BY f.user_id
                ORDER BY COUNT(f.user_id) DESC
                LIMIT 1            
            """)
            return cursor.fetchall()


with DAG(
        'DAG_10_sv-ljubushkina',
        default_args={
            'depends_on_past': False,
            'email': ['airflow@example.com'],
            'email_on_failure': False,
            'email_on_retry': False,
            'retries': 1,
            'retry_delay': timedelta(minutes=5),
        },
        description='DAG_10_sv-ljubushkina',
        schedule_interval=timedelta(days=1),
        start_date=datetime(2022, 4, 11),
        catchup=False,
        tags=['DAG_10_sv-ljubushkina'],
) as dag:
    t1 = PythonOperator(
        task_id="top_user",
        python_callable=task_1
    )

    t1