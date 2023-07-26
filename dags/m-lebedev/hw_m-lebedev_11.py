from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.hooks.base_hook import BaseHook
import psycopg2
from psycopg2.extras import RealDictCursor

connection = BaseHook.get_connection("startml_feed")
database = connection.schema
host = connection.host
user = connection.login
password = connection.password
port = connection.port
cursor_factory = RealDictCursor

def user_max_likes():
    with psycopg2.connect(
        database=database,
        host=host,
        user=user,
        password=password,
        port=port,
        cursor_factory=cursor_factory
    ) as conn:
        with conn.cursor() as cursor:
            cursor.execute("""                   
            SELECT user_id, COUNT(action)
            FROM feed_action
            WHERE action = 'like'
            GROUP BY user_id
            ORDER BY COUNT(action) DESC
            LIMIT 1
            """)
            result = cursor.fetchone()
            return result
            
with DAG(
    'hw_m-lebedev_11',
    default_args={
        'depends_on_past': False,
        'email': ['airflow@example.com'],
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 1,
        'retry_delay': timedelta(minutes=5),
    },
    description='Homework: 11, login: m-lebedev',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2023, 7, 24),
    catchup=False,
    tags=['m-lebedev'],
) as dag:

    task =  PythonOperator(
        task_id = 'most_likes',
        python_callable=user_max_likes,
    )