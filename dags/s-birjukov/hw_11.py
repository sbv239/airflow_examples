from datetime import timedelta, datetime

import psycopg2
from airflow import DAG

from airflow.operators.python import PythonOperator
from airflow.hooks.base import BaseHook
from psycopg2.extras import RealDictCursor


def con_with_base(ti):


    max_like = dict()
    creds = BaseHook.get_connection("startml_feed")
    with psycopg2.connect(
        f"postgresql://{creds.login}:{creds.password}"
        f"@{creds.host}:{creds.port}/{creds.schema}",
        cursor_factory=RealDictCursor
    ) as conn:
        with conn.cursor() as cursor:
            cursor.execute("""
            Select user_id, COUNT(action)
            from feed_action
            where action = 'like'
            group by(user_id)
            order by(COUNT(action)) DESC
            limit 1
            """
           )
            max_like = cursor.fetchone()
            return max_like
with DAG(
    'hw_11_s-birjukov',
    default_args={
        'depends_on_past': False,
        'email': ['airflow@example.com'],
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 1,
        'retry_delay': timedelta(minutes=5),
        },
        description = 'DAG_xcom',
        schedule_interval = timedelta(days = 1),
        start_date = datetime(2023,3,25),
        catchup=False,
        tags = ['hw_11_s-birjukov_connections']
) as dag:

    t1 = PythonOperator(
    task_id = 'find_user',
    python_callable = con_with_base,
    )