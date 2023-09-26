import psycopg2
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook

def get_from_base():
    postgres = PostgresHook(postgres_conn_id='startml_feed')
    with postgres.get_conn() as conn:
        with conn.cursor() as cursor:
            cursor.execute("""
                SELECT user_id, COUNT(user_id)
                FROM feed_action
                WHERE action = 'like'
                GROUP BY user_id
                ORDER BY COUNT(user_id) DESC
                LIMIT 1
            """)
            return cursor.fetchone()


with DAG(
    'hw_al-pivovarov_11',
    default_args={
        'depends_on_past': False,
        'email': ['airflow@example.com'],
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 1,
        'retry_delay': timedelta(minutes=5),  # timedelta из пакета datetime
    },
    start_date=datetime.now()
) as dag:
    t1 = PythonOperator(task_id='get_from_base', python_callable=get_from_base)