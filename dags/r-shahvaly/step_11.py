"""
step_11 DAG
"""
from datetime import datetime, timedelta
from psycopg2.extras import RealDictCursor

from airflow import DAG
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.operators.python import PythonOperator


with DAG(
    'hw_r-shahvaly_11',

    default_args={
        'depends_on_past': False,
        'email': ['airflow@example.com'],
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 1,
        'retry_delay': timedelta(minutes=5),
    },
    description='DAG for step_11',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2023, 1, 1),
    catchup=False,
    tags=['r-shahvaly'],
) as dag:

        def get_user():
            postgres = PostgresHook(postgres_conn_id="startml_feed")
            with postgres.get_conn() as conn:  # вернет тот же connection, что вернул бы psycopg2.connect(...)
                with conn.cursor(cursor_factory=RealDictCursor) as cursor:
                    cursor.execute(
                        """
                            SELECT user_id, COUNT(user_id)
                            FROM feed_action
                            WHERE action = 'like'
                            GROUP BY user_id
                            ORDER BY COUNT(user_id) DESC
                            LIMIT 1
                        """
                    )
                    return cursor.fetchone()


        task = PythonOperator(
                task_id="PythonOperator",
                python_callable=get_user,
        )

