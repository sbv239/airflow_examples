from datetime import datetime, timedelta
from textwrap import dedent
from psycopg2.extras import RealDictCursor

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresHook


def python_func0():

    pg_hook = PostgresHook(postgres_conn_id='startml_feed')
    most_active_user = """
    SELECT
        f.user_id,
        COUNT(f.user_id) as count
    FROM feed_action f
    WHERE f.action = 'like'
    GROUP BY f.user_id
    ORDER BY COUNT(f.user_id) DESC
    LIMIT 1
    """
    return pg_hook.get_first(most_active_user)

def python_func1():

    postgres = PostgresHook(postgres_conn_id='startml_feed')
    with postgres.get_conn() as conn:
        with conn.cursor(cursor_factory=RealDictCursor) as cursor:
            cursor.execute("""
            SELECT
                f.user_id,
                COUNT(f.user_id) as count
            FROM feed_action f
            WHERE f.action = 'like'
            GROUP BY f.user_id
            ORDER BY COUNT(f.user_id) DESC
            LIMIT 1
            """)
            return cursor.fetchone()


with DAG(
    's-filkin-7-dag9',
    default_args={
        'depends_on_past': False,
        'email': ['airflow@example.com'],
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 1,
        'retry_delay': timedelta(minutes=5),
    },
    schedule_interval=timedelta(days=1),
    start_date=datetime(2022, 4, 23),
) as dag:

    t1 = PythonOperator(
        task_id='t1', 
        python_callable=python_func1,

    )

    t1