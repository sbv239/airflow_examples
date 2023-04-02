from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta

from airflow.providers.postgres.operators.postgres import PostgresHook


def get_user_with_the_most_likes():
    postgres = PostgresHook(postgres_conn_id='startml_feed')
    with postgres.get_conn() as conn:   # вернет тот же connection, что вернул бы psycopg2.connect(...)
        with conn.cursor() as cursor:
            cursor.execute(
                """
                SELECT user_id, COUNT(action='like') AS count
                FROM "feed_action"
                WHERE action='like'
                GROUP BY 1
                ORDER BY 2 DESC
                LIMIT 1
                """)
            return dict(cursor.fetchone())


# Default settings applied to all tasks
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}

with DAG(
    'hw_11_a-korenevich-1',
    start_date=datetime(2021, 1, 1),
    max_active_runs=2,
    schedule_interval=timedelta(minutes=30),
    default_args=default_args,
    catchup=False,
    tags=['a-korenevich-1']
) as dag:
    t1 = PythonOperator(
        task_id = 'user_with_the_most_likes_id',
        python_callable=get_user_with_the_most_likes
    )
