from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta
from airflow.hooks.base import BaseHook
from airflow.providers.postgres.operators.postgres import PostgresHook


def search_user():
    postgres = PostgresHook(postgres_conn_id="startml_feed")
    with postgres.get_conn() as conn:
        with conn.cursor() as cursor:
            cursor.execute(
                """
                SELECT user_id,count(*) from feed_action
                WHERE action = 'like'
                GROUP BY user_id
                ORDER BY count(*) desc
                LIMIT 1
                """
            )
            return cursor.fetchall()

with DAG(
        'hw_11_i-kulikov-17',
        default_args={
            'depends_on_past': False,
            'email': ['airflow@example.com'],
            'email_on_failure': False,
            'email_on_retry': False,
            'retries': 1,
            'retry_delay': timedelta(minutes=5),
        },
        description='sql connect',
        schedule_interval=timedelta(days=1),
        start_date=datetime(2023, 2, 10),
        catchup=False,
        tags=['hw_11_i-kulikov-17']
) as dag:

    t1 = PythonOperator(
        task_id = 'search_user',
        python_callable=search_user,
    )

    t1
