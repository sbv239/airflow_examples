from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.hooks.base import BaseHook

import psycopg2
from psycopg2.extras import RealDictCursor

# from airflow.providers.postgres.hooks.postgres import PostgresHook


def find_user():

    creds = BaseHook.get_connection("startml_feed")
    with psycopg2.connect(
            f"postgresql://{creds.login}:{creds.password}"
            f"@{creds.host}:{creds.port}/{creds.schema}",
            cursor_factory=RealDictCursor
    ) as conn:
        with conn.cursor() as cursor:
            cursor.execute(
                """
                SELECT user_id, COUNT(*)
                FROM feed_action
                WHERE action = 'like'
                GROUP BY user_id
                ORDER BY COUNT(*) DESC
                LIMIT 1
                """)
            results = cursor.fetchall()

    return results


with DAG(
        "hw_m-golovaneva_task11",

        default_args={
            'depends_on_past': False,
            'email': ['airflow@example.com'],
            'email_on_failure': False,
            'email_on_retry': False,
            'retries': 1,
            'retry_delay': timedelta(minutes=5),
        },
        description='Connections in Airflow (to Postgres)',
        schedule_interval=timedelta(days=1),
        start_date=datetime(2022, 1, 1),
        catchup=False,

        tags=['mariaSG']
) as dag:

    which_user = PythonOperator(
        task_id="which_user",
        python_callable=find_user
    )







