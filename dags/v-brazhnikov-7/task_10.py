from datetime import timedelta

from psycopg2.extras import RealDictCursor
from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.operators.python_operator import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresHook


with DAG(
    "task_10",
    default_args={
        "depends_on_past": False,
        "email": ["airflow@example.com"],
        "email_on_failure": False,
        "email_on_retry": False,
        "retries": 1,
        "retry_delay": timedelta(minutes=5),
        "start_date": days_ago(2),
    },
    catchup=False,
) as dag:

    def get_max_likes_user():
        postgres = PostgresHook(postgres_conn_id="startml_feed")
        with postgres.get_conn() as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cursor:
                cursor.execute(
                    """
                    SELECT user_id, COUNT(*)
                    FROM feed_action 
                    WHERE feed_action.action = 'like'
                    GROUP BY user_id
                    ORDER BY COUNT(*) DESC
                    LIMIT 1
                    """
                )
                return cursor.fetchone()

    PythonOperator(
        task_id="max_like",
        python_callable=get_max_likes_user,
        dag=dag,
    )
