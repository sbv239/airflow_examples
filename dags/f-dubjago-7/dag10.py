"""
Connections
"""
from airflow import DAG
from datetime import datetime, timedelta

from airflow.operators.python import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresHook

from psycopg2.extras import RealDictCursor

default_args = {
    'depends_on_past': False,
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),  # timedelta из пакета datetime
}


def find_top_likes_user():
    postgres = PostgresHook(postgres_conn_id="startml_feed")
    with postgres.get_conn() as conn:
        with conn.cursor(cursor_factory=RealDictCursor) as cursor:
            cursor.execute("""                   
    SELECT user_id, COUNT(user_id) as count
    FROM feed_action 
    WHERE action = 'like'
    GROUP BY user_id 
    ORDER BY count DESC LIMIT 1
      """)
            result = cursor.fetchone()
        return result


with DAG(
        'f-dubjago-7_dag10',
        default_args=default_args,
        schedule_interval=timedelta(days=1),
        start_date=datetime(2022, 4, 10),
        tags=['df'],
) as dag:
    find_top_likes_user = PythonOperator(
        task_id='find_top_likes_user',
        python_callable=find_top_likes_user,
    )
    find_top_likes_user
