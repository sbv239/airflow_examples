from datetime import datetime, timedelta
from textwrap import dedent
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator


def get_connection():
    from airflow.providers.postgres.hooks.postgres import PostgresHook

    postgres = PostgresHook(postgres_conn_id='startml_feed')
    with postgres.get_conn() as conn:
        with conn.cursor() as cursor:
            cursor.execute(
                """
                SELECT user_id, COUNT(action)
                FROM feed_action
                WHERE action = 'like'
                GROUP BY user_id
                ORDER BY COUNT(action) DESC
                LIMIT 1
                """
            )
            return cursor.fetchall()
with DAG(
    'hw_s-majkova_11',
    default_args = {
    'depens_on_past': False,
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes = 5),
    },
    description = 'first_dynamic_DAG',
    schedule_interval=timedelta(days=1),
    start_date = datetime(2023, 11, 25),
    catchup = False,
    tags = ['hw_11'],
) as dag:
    t1 = PythonOperator(
        task_id = 'likes',
        python_callable = get_connection
    )
t1
