from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresHook

from psycopg2.extras import RealDictCursor

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

            res = cursor.fetchone()

            return res


with DAG(
    'aloshkarev_task_9',
    default_args = {
        'depends_on_past': False,
        'email': ['airflow@example.com'],
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 1,
        'retry_delay': timedelta(minutes=5),  # timedelta из пакета datetime
    },
    schedule_interval=timedelta(days=1),
    start_date=datetime(2022, 4, 12),
    catchup=False,
) as dag:
    t1 = task = PythonOperator(
            task_id='a_loshkarev_10_1',
            python_callable=task_1
        )

    t1
