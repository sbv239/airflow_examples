from datetime import datetime, timedelta
from airflow import DAG
from psycopg2.extras import RealDictCursor
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresHook

postgres = PostgresHook(postgres_conn_id="startml_feed")

default_args = {
    'depends_on_past': False,
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}


def user_max_like():
    with postgres.get_conn() as conn:
        with conn.cursor(cursor_factory=RealDictCursor) as cursor:
            cursor.execute(
                """
                SELECT f.user_id, COUNT(f.user_id)
                FROM feed_action f
                WHERE f.action = 'like'
                GROUP BY f.user_id
                ORDER BY COUNT(f.user_id) DESC
                LIMIT 1
                """
            )
            results = cursor.fetchone()
    return results


with DAG(
        "hw_11_n-efremov",
        default_args=default_args,
        description='hw_11_n-efremov',
        schedule_interval=timedelta(days=1),
        start_date=datetime(2022, 10, 29),
        catchup=False,
        tags=['n-efremov'],
) as dag:
    task = PythonOperator(
        task_id="user_max_like",
        python_callable=user_max_like
    )
    task
