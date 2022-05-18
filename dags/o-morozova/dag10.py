"""
Test documentation
"""
from datetime import datetime, timedelta
from textwrap import dedent

from airflow import DAG
from airflow.operators.python_operator import PythonOperator

from airflow.providers.postgres.operators.postgres import PostgresHook
import psycopg2
from psycopg2.extras import RealDictCursor

with DAG(
    '10_omorozova',
    default_args={
        'depends_on_past': False,
        'email': ['airflow@example.com'],
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 1,
        'retry_delay': timedelta(minutes=5),  # timedelta из пакета datetime
    },
    description='10_omorozova',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2022, 1, 1),
    catchup=False,
    tags=['10_omorozova'],
) as dag:
        date = "{{ ds }}"
        def user_max_likes():
                postgres = PostgresHook(postgres_conn_id="startml_feed")
                with postgres.get_conn() as conn:
                        with conn.cursor(cursor_factory=RealDictCursor) as cursor:
                                cursor.execute(
                                        """SELECT f.user_id, count(f.action) as count
                                             FROM "feed_action" f
                                            WHERE f.action = 'like'
                                            GROUP BY 1
                                            ORDER BY 2 DESC
                                            LIMIT 1
                                               """)
                                f = cursor.fetchall()
                                res = {'user_id': f['user_id'], 'count': f['count']}
                return res


        t1 = PythonOperator(
                task_id='user_max_likes',
                python_callable=user_max_likes,
        )

