from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from airflow.providers.postgres.hooks.postgres import PostgresHook
from psycopg2.extras import RealDictCursor


def user_by_likes():
    postgres = PostgresHook(postgres_conn_id="startml_feed")
    with postgres.get_conn() as conn:
        with conn.cursor(cursor_factory=RealDictCursor) as cursor:
            cursor.execute(
                """
                SELECT user_id, COUNT(user_id) FROM "feed_action"
                WHERE action = 'like' GROUP BY user_id
                ORDER BY COUNT(user_id) DESC
                LIMIT 1
                """
            )
            return cursor.fetchone()

default_args = {
    'depends_on_past': False,
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG('hw_g-vinokurov_10',
    default_args=default_args,
        description='DAG in task_10',
        schedule_interval=timedelta(days=1),
        start_date=datetime(2023, 9, 29),
        catchup=False,
) as dag:

    operator = PythonOperator(
        task_id='user_by_likes',
        python_callable=user_by_likes,
    )

    operator