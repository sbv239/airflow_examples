"""
DAG for task 11
"""
from datetime import timedelta, datetime

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from psycopg2.extras import RealDictCursor

with DAG(
        'hw_t-togyzbaev_11',
        default_args={
            'depends_on_past': False,
            'email': ['airflow@example.com'],
            'email_on_failure': False,
            'email_on_retry': False,
            'retries': 1,
            'retry_delay': timedelta(minutes=5),  # timedelta из пакета datetime
        },
        schedule_interval=timedelta(days=1),
        start_date=datetime(2022, 1, 1),
        catchup=False,
        tags=['t-togyzbaev']
) as dag:
    def find_user_liked_most():
        postgres = PostgresHook(postgres_conn_id="startml_feed")
        with postgres.get_conn() as conn:   # вернет тот же connection, что вернул бы psycopg2.connect(...)
            with conn.cursor(cursor_factory=RealDictCursor) as cursor:
                cursor.execute(
                    """
                    select user_id, count(action) 
                    from "feed_action" 
                    where action = 'like' 
                    group by user_id 
                    order by count(action) desc 
                    """
                )
                return cursor.fetchone()

    t1 = PythonOperator(
        task_id="find_user_liked_most",
        python_callable=find_user_liked_most
    )
