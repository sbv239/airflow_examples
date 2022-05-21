from datetime import datetime, timedelta
from textwrap import dedent
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresHook
import psycopg2
from psycopg2.extras import RealDictCursor

with DAG(
        'DAG_10',
        default_args={
            'depends_on_past': False,
            'email': ['airflow@example.com'],
            'email_on_failure': False,
            'email_on_retry': False,
            'retries': 1,
            'retry_delay': timedelta(minutes=5),  # timedelta из пакета datetime
        },
        description='lesson_11_task_10',
        schedule_interval=timedelta(days=1),
        start_date=datetime(2022, 1, 1),
        catchup=False,
        tags=['v-mashir-8'],
) as dag:

    def likes():
        postgres = PostgresHook(postgres_conn_id="startml_feed")
        with postgres.get_conn() as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cursor:
                cursor.execute(
                    """SELECT user_id, COUNT(action) as COUNT
                                            FROM "feed_action" 
                                            WHERE action = 'like'
                                            GROUP BY user_id
                                            ORDER BY COUNT DESC
                                            LIMIT 1
                                        """
                )
                res = cursor.fetchone()
        return res


    t1 = PythonOperator(
        task_id='likes',
        python_callable=likes,
    )
