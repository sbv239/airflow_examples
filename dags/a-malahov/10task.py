from datetime import datetime, timedelta
from textwrap import dedent
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresHook
import psycopg2
from psycopg2.extras import RealDictCursor

with DAG(
        'a-malahov_task10',
        default_args={
            'depends_on_past': False,
            'email': ['airflow@example.com'],
            'email_on_failure': False,
            'email_on_retry': False,
            'retries': 1,
            'retry_delay': timedelta(minutes=5),  # timedelta из пакета datetime
        },
        description='a-malachov task 10',
        schedule_interval=timedelta(days=1),
        start_date=datetime(2022, 5, 10),
        catchup=False,
        tags=['malahov'],
) as dag:

    def conn_pg():
        conn_id = "startml_feed"
        postgres = PostgresHook(postgres_conn_id=conn_id)
        with postgres.get_conn() as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cursor:
                cursor.execute(
                    """
                    SELECT user_id, count(post_id)
                    FROM feed_action 
                    WHERE action = 'like'
                    GROUP BY user_id
                    ORDER BY count(post_id)
                    DESC LIMIT 1
                    """
                )
                return cursor.fetchall()

    max_like = PythonOperator(
        task_id='max_like_id',
        python_callable=conn_pg,
    )

    max_like
