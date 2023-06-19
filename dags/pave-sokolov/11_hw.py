from datetime import datetime, timedelta
from textwrap import dedent

from airflow import DAG

from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

from airflow.hooks.base import BaseHook

import psycopg2
from psycopg2.extras import RealDictCursor


creds = BaseHook.get_connection("startml_feed")

def user_info():
    with psycopg2.connect(
        f"postgresql://{creds.login}:{creds.password}"
        f"@{creds.host}:{creds.port}/{creds.schema}",
        cursor_factory= RealDictCursor
        ) as conn:
        with conn.cursor() as cursor:
    
            cursor.execute(
            f"""
            SELECT user_id, COUNT(action)
            FROM feed_action
            WHERE action = 'like'
            GROUP BY user_id
            ORDER BY COUNT(action) DESC
            LIMIT 1
            """
            )
            return (dict(cursor.fetchone()))
        



with DAG ('hw_pave-sokolov_11',
          default_args={
            'depends_on_past': False,
            'email': ['airflow@example.com'],
            'email_on_failure': False,
            'email_on_retry': False,
            'retries': 1,
            'retry_delay': timedelta(minutes=5), 
            },
            description= 'HW 11 DAG',
            schedule_interval=timedelta(days = 1),
            start_date= datetime(2023,6,12),
            catchup= False,
            tags= ['example']
        ) as dag:
    
    task = PythonOperator(
        task_id = 'request_of_the_most_likely_user',
        python_callable= user_info
    )

    task