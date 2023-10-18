from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from psycopg2.extras import RealDictCursor


default_args = {
    'depends_on_past': False,
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}


def get_top_likes_user():
    postgres = PostgresHook(postgres_conn_id="startml_feed")
    with postgres.get_conn() as conn:
        with conn.cursor(cursor_factory=RealDictCursor) as cursor:
            cursor.execute("""
                SELECT
                    fa.user_id,
                    COUNT(fa.post_id) as count
                FROM feed_action as fa
                WHERE fa.action = 'like'
                GROUP BY fa.user_id
                ORDER BY count DESC
                LIMIT 1
            """)
            result = cursor.fetchone()
            return result


with DAG(
    'hw_alekse-smirnov_11',
    default_args=default_args,
    description='DAG for Lesson #11 Task #11',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2023, 10, 16),
    catchup=False,
    tags=['alekse-smirnov'],
) as dag:

    get_user_task = PythonOperator(
        task_id="get_top_likes_user_task",
        python_callable=get_top_likes_user
    )
