from airflow.providers.postgres.hooks.postgres import PostgresHook
import psycopg2

from datetime import datetime, timedelta
from textwrap import dedent

from airflow import DAG


from airflow.operators.python import PythonOperator

def likes_BD():
    from psycopg2.extras import RealDictCursor
    postgres = PostgresHook(postgres_conn_id="startml_feed")
    with postgres.get_conn() as conn:   # вернет тот же connection, что вернул бы psycopg2.connect(...)
        with conn.cursor(cursor_factory=RealDictCursor) as cursor:
    
            cursor.execute("""SELECT user_id,
                                          COUNT(user_id)
                                     FROM "feed_action" 
                                     WHERE action = 'like'
                                     GROUP BY user_id
                                     ORDER BY COUNT(user_id) DESC
                                     LIMIT 1 """)

            return cursor.fetchone()
             




with DAG(
    'hw_ta-korobitsyna_11',
    default_args={
        'depends_on_past': False,
        'email': ['airflow@example.com'],
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 1,
        'retry_delay': timedelta(minutes=5), 
        },
    description='hw_11_ta-korobitsyna',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2023, 9, 19),
    catchup=False,
    tags=['hw_ta-korobitsyna_11'],
) as dag:
           
    t1 = PythonOperator(
            task_id="BD_task_11",  # нужен task_id, как и всем операторам
            python_callable=likes_BD,
            provide_context = True
            )