from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from airflow.providers.postgres.operators.postgres import PostgresHook

from airflow.hooks.base import BaseHook
import psycopg2
from psycopg2.extras import RealDictCursor

'''  
def get_user_max_likes():
    creds = BaseHook.get_connection("startml_feed")
    with psycopg2.connect(
        f"postgresql://{creds.login}:{creds.password}"
        f"@{creds.host}:{creds.port}/{creds.schema}", cursor_factory=RealDictCursor
    ) as conn:
        with conn.cursor() as cursor:
            cursor.execute(
                """
                SELECT f.user_id, COUNT(f.user_id)
                FROM feed_action as f
                WHERE f.action = 'like'
                GROUP BY f.user_id
                ORDER BY COUNT(f.user_id) DESC
                LIMIT 1        
                """
            )
            return cursor.fetchone()
'''
def get_user_max_likes():
    postgres = PostgresHook(postgres_conn_id="startml_feed")
    with postgres.get_conn() as conn:   
        with conn.cursor() as cursor:
            cursor.execute(
                """
                SELECT f.user_id, COUNT(f.user_id)
                FROM feed_action as f
                WHERE f.action = 'like'
                GROUP BY f.user_id
                ORDER BY COUNT(f.user_id) DESC
                LIMIT 1        
                """
            )
            return cursor.fetchall()
          
with DAG(
    'hw11_11_r-safarov',
    default_args={
        'depends_on_past': False,
        'email': ['airflow@example.com'],
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 1,
        'retry_delay': timedelta(minutes=5)
    },
    description='Task_11',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2023, 1, 1),
    catchup=False,
    tags=['hw_11_r-safarov']
) as dag:
    t1 = PythonOperator(
        task_id="user_max_likes",
        python_callable=get_user_max_likes
    )

t1