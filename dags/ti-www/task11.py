from datetime import datetime, timedelta
from textwrap import dedent

from airflow.hooks.base import BaseHook
import psycopg2
from psycopg2.extras import RealDictCursor

from airflow import DAG

from airflow.operators.python import PythonOperator

def connect_to_database():
    creds = BaseHook.get_connection("startml_feed")
    with psycopg2.connect(
        f"postgresql://{creds.login}:{creds.password}"
        f"@{creds.host}:{creds.port}/{creds.schema}"
    ) as conn:
        with conn.cursor(cursor_factory=RealDictCursor) as cursor:
            cursor.execute("""
            SELECT user_id, count(action) as count
            FROM feed_action
            WHERE action = 'like'
            GROUP BY user_id
            ORDER BY 2 DESC
            LIMIT 1
            """)
            result = cursor.fetchone()
    return result

with DAG(
    "ti-www_task11",
    default_args={
        'depends_on_past': False,
        'email': ['airflow@example.com'],
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 1,
        'retry_delay': timedelta(minutes=5),
    },
    description="DAG_test_connections",
    schedule_interval=timedelta(days=1),
    start_date=datetime(2023, 3, 19),
    catchup=False,
    tags=["ti-www"],
) as dag:

    t = PythonOperator(
        task_id="get_user_max_like",
        python_callable=connect_to_database,
        
    )

t