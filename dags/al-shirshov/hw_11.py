from datetime import datetime, timedelta
from textwrap import dedent
from airflow import DAG

from airflow.operators.bash import BashOperator
from airflow.operators.python_operator import PythonOperator
from airflow.hooks.base import BaseHook
from airflow.providers.postgres.hooks.postgres import PostgresHook
import psycopg2
import psycopg2.extras
from psycopg2.extras import RealDictCursor


def db_value():
    postgres = PostgresHook(postgres_conn_id="startml_feed")
    with postgres.get_conn() as conn:   # вернет тот же connection, что вернул бы psycopg2.connect(...)
        with conn.cursor(cursor_factory = RealDictCursor) as cursor:
            cursor.execute(
                """
                SELECT user_id, COUNT(*)
                FROM feed_action
                WHERE action='like'
                GROUP BY user_id
                ORDER BY COUNT(*) DESC
                LIMIT (1)
                """
            )
            return cursor.fetchone()

with DAG(
    "hw_al-shirshov_10",
    default_args={
    'depends_on_past': False,
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),  # timedelta из пакета datetime
},
    start_date=datetime(2023,5,20),
    catchup=True   
) as dag:
    t1 = PythonOperator(
        task_id = "db_query",
        python_callable = db_value
    )