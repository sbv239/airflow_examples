from datetime import datetime, timedelta
from airflow import DAG
from textwrap import dedent
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from psycopg2.extras import RealDictCursor

with DAG(
    'hw_g-hristov_11',
    default_args={
        'depends_on_past': False,
        'email': ['airflow@example.com'],
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 1,
        'retry_delay': timedelta(minutes=5),  # timedelta из пакета datetime
    },
    description='task11',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2023, 11, 28),
    catchup=False,
) as dag:

    def connec():
        postgres = PostgresHook(postgres_conn_id="startml_feed")
        with postgres.get_conn() as conn:  # вернет тот же connection, что вернул бы psycopg2.connect(...)
            with conn.cursor(cursor_factory=RealDictCursor) as cursor:
                cursor = conn.cursor()
                cursor.execute(
                    """
                    SELECT user_id, COUNT(action) as count
                    FROM "feed_action"
                    WHERE action = 'like'
                    GROUP BY user_id
                    ORDER BY count DESC
                    LIMIT 1
                    """
                )
                return cursor.fetchone()

    taskpo = PythonOperator(
        task_id='hw_g-hristov_11_PO',
        python_callable=connec,
    )
