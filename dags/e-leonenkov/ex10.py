"""
DAG: getting postgres connection
"""
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator


def get_connection():
    from airflow.providers.postgres.operators.postgres import PostgresHook
    from psycopg2.extras import RealDictCursor

    postgres = PostgresHook(postgres_conn_id="startml_feed")
    with postgres.get_conn() as conn:
        with conn.cursor(cursor_factory=RealDictCursor) as cursor:
            cursor.execute("""
                SELECT fa.user_id, COUNT(fa.action) as count
                FROM feed_action fa
                WHERE fa.action = 'like'
                GROUP BY fa.user_id
                ORDER BY COUNT(fa.action) DESC
            """)
            return cursor.fetchone()


with DAG(
    'hw_10_e-leonenkov',

    default_args={
    'depends_on_past': False,
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    },
    description='DAG with postgres connection',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2022, 1, 1),
    catchup=False,
    tags=['ex10'],
) as dag:
    t1 = PythonOperator(
        task_id='posgres_connection',
        python_callable=get_connection
    )

    t1