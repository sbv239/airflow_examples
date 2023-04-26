"""
Test documentation
"""
from datetime import datetime, timedelta
from textwrap import dedent

from airflow import DAG

from airflow.operators.python_operator import PythonOperator
from airflow.operators.bash import BashOperator

from airflow.providers.postgres.operators.postgres import PostgresHook


def sql_request():
    postgres = PostgresHook(postgres_conn_id="startml_feed")
    with postgres.get_conn() as conn:
        with conn.cursor() as cursor:
            cursor.execute(
                """
                SELECT user_id, count(*) from feed_action
                WHERE action = 'like'
                GROUP BY user_id
                ORDER BY count(*) desc
                LIMIT 1
                """
            )
            return cursor.fetchall()
        

# def sql_request():
#         from airflow.hooks.base import BaseHook
#         import psycopg2
#         from psycopg2.extras import RealDictCursor
#
#         creds = BaseHook.get_connection('startml_feed')
#         with psycopg2.connect(
#                 f"postgresql://{creds.login}:{creds.password}"
#                 f"@{creds.host}:{creds.port}/{creds.schema}",
#
#         ) as conn:
#                 with conn.cursor(
#                         cursor_factory=RealDictCursor
#                 ) as cursor:
#                         cursor.execute(
#                                 """
#                                 SELECT id, COUNT(action) FROM "user" u
#                                 JOIN "feed_action" f ON u.id = f.user_id
#                                 WHERE action = 'like'
#                                 GROUP by id
#                                 ORDER BY COUNT(action) DESC
#                                 LIMIT 1
#                                  """)
#                         return cursor.fetchone()

with DAG(
    'hw_11_a_loskutov',
    # Параметры по умолчанию для тасок

    default_args={
        'depends_on_past': False,
        'email': ['airflow@example.com'],
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 1,
        'retry_delay': timedelta(minutes=5),  # timedelta из пакета datetime
    },

    description='My first DAG',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2023, 4, 25),
    catchup=False,
    tags=['Loskutov_hm'],
) as dag:    

        first = PythonOperator(
                task_id='return_sql_request',
                python_callable=sql_request,
        )

        first

