from datetime import datetime, timedelta
from textwrap import dedent

# Для объявления DAG нужно импортировать класс из airflow
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook

def select_likes():
    db = PostgresHook(postgres_conn_id="startml_feed")
    with db.get_conn() as conn:
        with conn.cursor() as cursor:
            cursor.execute(
                """
                SELECT user_id, COUNT(action) AS count
                FROM feed_action
                WHERE action = 'like'
                GROUP BY user_id
                ORDER BY count DESC
                LIMIT 1
                """
            )
            return cursor.fetchone()

with DAG(
    "task_11_k-zhuravlev",

default_args={
    'depends_on_past': False,
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),  # timedelta из пакета datetime
},

    start_date=datetime.now(),
    tags=["Cool_tag"]
) as dag:
    t1 = PythonOperator(
        task_id="query",
        python_callable=select_likes
    )

    t1