from datetime import datetime, timedelta
import json

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.hooks.base import BaseHook

import psycopg2
from psycopg2.extras import RealDictCursor


def most_likes():
    creds = BaseHook.get_connection("startml_feed")
    with psycopg2.connect(
        f"postgresql://{creds.login}:{creds.password}"
        f"@{creds.host}:{creds.port}/{creds.schema}",
        cursor_factory=RealDictCursor
    ) as conn:
        with conn.cursor() as cursor:
            cursor.execute(
                """
                SELECT f.post_id, COUNT(f.post_id)
                FROM feed_action f
                WHERE f.action = 'like'
                GROUP BY f.post_id
                ORDER BY COUNT(f.post_id) DESC
                LIMIT 10
                ;
                """
                )
            result = cursor.fetchone()
            return json.dumps(result)


with DAG(
    'hw_arse-beljaev_9',
    default_args={
        'depends_on_past': False,
        'email': ['airflow@example.com'],
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 1,
        'retry_delay': timedelta(minutes=5)
        },
    description='hw_9_lesson_11',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2023, 9, 18),
    catchup=False,
    tags=['example']
) as dag:

    t1 = PythonOperator(
        task_id='pulling_data',
        python_callable=most_likes,
    )

    t1
