from airflow.providers.postgres.hooks.postgres import PostgresHook
from psycopg2.extras import RealDictCursor
import psycopg2
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

def get_data():
    postgres = PostgresHook(postgres_conn_id="startml_feed")
    with postgres.get_conn() as conn:
        with conn.cursor(cursor_factory=RealDictCursor) as cursor:
            cursor.execute(
                '''
                SELECT user_id, COUNT(action)
                FROM feed_action
                WHERE action = 'like'
                GROUP BY user_id
                ORDER BY COUNT(action) DESC
                LIMIT 1
                '''
            )
            return cursor.fetchone()

with DAG(
    'hw_ni-nikitina_11', 
    default_args={
        'depends_on_past': False,
        'email': ['airflow@example.com'],
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 1,
        'retry_delay': timedelta(minutes=5),
    },
    description='Eleventh Task',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2023, 11, 29),
    catchup=False
) as dag:
    
    t1 = PythonOperator(
        task_id='data', 
        python_callable=get_data
    )


