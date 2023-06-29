from airflow import DAG
from datetime import datetime, timedelta
from airflow.operators.python_operator import PythonOperator
from psycopg2.extras import RealDictCursor
from airflow.providers.postgres.hooks.postgres import PostgresHook

def find_user():
    postgres = PostgresHook(postgres_conn_id='startml_feed')
    with postgres.get_conn() as conn:   # вернет тот же connection, что вернул бы psycopg2.connect(...)
        with conn.cursor() as cursor:
            cursor.execute(
                """SELECT user_id, COUNT(action)
                FROM feed_action
                WHERE action = 'like'
                GROUP BY user_id
                ORDER BY COUNT(user_id) DESC
                LIMIT 1
                """
            )
            result = cursor.fetchone()
        return result
with DAG (
    'hw_d-orlova-21_11',
    default_args={
        'depends_on_past': False,
        'email': ['airflow@example.com'],
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 1,
        'retry_delay': timedelta(minutes=5),  # timedelta из пакета datetime
    },
    description = 'dag for lesson 11.11',
    schedule_interval = timedelta(days=1),
    start_date=datetime(2023, 6, 28),
    catchup = False
) as dag:

    task = PythonOperator(
        task_id = 'find_user_postgres',
        python_callable = find_user
    )