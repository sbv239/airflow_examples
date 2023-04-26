from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

from datetime import datetime, timedelta
from textwrap import dedent

from airflow.providers.postgres.operators.postgres import PostgresHook
from airflow.hooks.base import BaseHook
import psycopg2
from psycopg2.extras import RealDictCursor

with DAG(
    'hw_11_al-savelev',
    default_args={
        'depends_on_past': False,
        'email': ['airflow@example.com'],
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 1,
        'retry_delay': timedelta(minutes=5)
    },
    description='test_11_11',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2023, 4, 1),
    catchup=False,
    tags=['hw_al-savelev']
) as dag:

    def get_user():
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
  
    t1 = PythonOperator(
        task_id='hw_11_al-savelev_1',
        python_callable=get_user
        )

    t1
