from datetime import datetime, timedelta
from textwrap import dedent

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

from airflow.providers.postgres.hooks.postgres import PostgresHook

with DAG(
    'ratibor_task11',
    start_date=datetime(2023, 5, 11),
    default_args={
        'depends_on_past': False,
        'email': ['airflow@example.com'],
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 1,
        'retry_delay': timedelta(minutes=5),
    },
) as dag:
    
    def get_most_liking_user():
        postgres = PostgresHook(postgres_conn_id="startml_feed")
        with postgres.get_conn() as conn:
            with conn.cursor() as cursor:
                cursor.execute(
                    """
                    SELECT interim_table.user_id, interim_table.like_count
                    FROM(
                        SELECT user_id AS user_id, COUNT(action) AS like_count
                        FROM feed_action
                        WHERE action = 'like'
                        GROUP BY user_id
                    ) AS interim_table
                    ORDER BY interim_table.like_count DESC
                    LIMIT 1
                    """
                )
                return dict(cursor.fetchone())
            
    most_liking_user_getter = PythonOperator(
        task_id="get_most_liking_user",
        python_callable=get_most_liking_user
    )