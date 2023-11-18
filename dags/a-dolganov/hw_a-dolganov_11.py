from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator

from airflow.hooks.base import BaseHook
import psycopg2
from psycopg2.extras import RealDictCursor

def get_top():
    
    connection = BaseHook.get_connection("startml_feed")
    with psycopg2.connect(
        f"postgresql://robot-startml-ro:pheiph0hahj1Vaif"
        f"@postgres.lab.karpov.courses:6432/startml",
        cursor_factory=RealDictCursor
    ) as conn:
        with conn.cursor() as cursor:
            cursor.execute("""
            SELECT f.user_id, COUNT(f.user_id)
            FROM feed_action f
            WHERE f.action = 'like'
            GROUP BY f.user_id
            ORDER BY COUNT(f.user_id) DESC
            LIMIT 10
            """)
            return cursor.fetchone()

with DAG(
    'hw_a-dolganov_11',
    default_args={
        'depends_on_past': False,
        'email': ['airflow@example.com'],
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 1,
        'retry_delay': timedelta(minutes=5),
    },
    description='HW 11 a-dolganov',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2023, 1, 1),
    catchup=False,
    tags=['a-dolganov'],
) as dag:

    task = PythonOperator(
            task_id ='get_top',
            python_callable = get_top
    )
    
    task