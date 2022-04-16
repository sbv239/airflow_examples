from datetime import timedelta, datetime

from airflow import DAG
from airflow.operators.python import PythonOperator


def get_query():
    from airflow.providers.postgres.operators.postgres import PostgresHook

    postgres = PostgresHook(postgres_conn_id='startml_feed')

    with postgres.get_conn() as conn:
        with conn.cursor() as cursor:
            cursor.execute(
                """
               SELECT user_id, COUNT(action)
               FROM feed_action
               WHERE action = 'like'
               GROUP BY user_id
               ORDER BY COUNT(action) DESC
               LIMIT 1
                """
            )
            return cursor.fetchone()


with DAG(
        'hw_9_a-djachkov',
        default_args={
            'depends_on_past': False,
            'email': ['airflow@example.com'],
            'email_on_failure': False,
            'email_on_retry': False,
            'retries': 1,
            'retry_delay': timedelta(minutes=5),
        },
        description="Lesson 11 home work 9",
        schedule_interval=timedelta(days=1),
        start_date=datetime(2022, 1, 1),
        catchup=False,
        tags=['a-djachkov'],
) as dag:
    task = PythonOperator(
        task_id='max_like_per_user',
        python_callable=get_query
    )
    task
