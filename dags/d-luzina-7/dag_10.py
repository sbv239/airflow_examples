from datetime import datetime, timedelta
from textwrap import dedent
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python_operator import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresHook
import psycopg2


def get_user():
    postgres = PostgresHook(postgres_conn_id="startml_feed")
    with postgres.get_conn() as conn:
        with conn.cursor() as cursor:
            cursor.execute(
                """
                SELECT user_id, COUNT(action)
                FROM "feed_action" 
                WHERE "action" = 'like'
                GROUP BY user_id
                ORDER BY COUNT(action) DESC
                LIMIT 1
                """
            )
            return cursor.fetchall()

with DAG(
    'dag_10_d-luzina-7',
    default_args={
        'depends_on_past': False,
        'email': ['airflow@example.com'],
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 1,
        'retry_delay': timedelta(minutes=5),
    },
    description='dag 10 lesson 11',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2022, 4, 17),
    catchup=False,
    tags=['dag_10_d-luzina-7'],
) as dag:
    
    t1 = PythonOperator(
        task_id = 'top_user_by_likes',
        python_callable=get_user
    )
                
    t1