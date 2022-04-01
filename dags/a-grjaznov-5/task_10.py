from textwrap import dedent
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta

def search_max():
    from airflow.providers.postgres.operators.postgres import PostgresHook

    postgres = PostgresHook(postgres_conn_id="startml_feed")
    with postgres.get_conn() as conn:
        with conn.cursor() as cursor:
            cursor.execute(
                """
                SELECT user_id, COUNT(action) AS sum_of__likes
                FROM "feed_action"
                WHERE action = 'like'
                GROUP BY user_id
                ORDER BY sum_of_likes DESC
                LIMIT 1
                """
            )
            return cursor.fetchall()


with DAG(
    'task_10_grjaznov',
        default_args={
            'depends_on_past': False,
            'email': ['airflow@example.com'],
            'email_on_failure': False,
            'email_on_retry': False,
            'retries': 1,
            'retry_delay': timedelta(minutes=5)
        },
    description='task_10_DAG',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2022, 3, 31),
    catchup=False,
    tags=['hw_10_a-grjaznov-5'],
) as dag:

    t1 = PythonOperator(
        task_id='max_likes_user',
        python_callable=search_max
    )

    t1
