"""
Start-ml Airflow Task 10
"""
from datetime import datetime, timedelta
from textwrap import dedent

from airflow import DAG

from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

from airflow.providers.postgres.operators.postgres import PostgresHook
from psycopg2.extras import RealDictCursor

def user_likes():
    postgres = PostgresHook(postgres_conn_id="startml_feed")
    with postgres.get_conn() as conn:
        with conn.cursor(cursor_factory=RealDictCursor) as cursor:
            cursor.execute(
                f"""
                SELECT f.user_id, count(f.action)
                FROM "feed_action" f
                WHERE f.action = 'like'
                GROUP BY f.user_id
                ORDER BY COUNT(f.action) DESC
                LIMIT 10
                """,
            )
            c = cursor.fetchone()

    return c

default_args={
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    'hw_10_o-zamoschin',
    default_args=default_args,
    schedule_interval=timedelta(days=1),
    start_date=datetime(2022, 1, 1),
    catchup=False,
) as dag:
    
    t1 = PythonOperator(
        task_id = 'get_user',
        python_callable=user_likes,
    )

    t1