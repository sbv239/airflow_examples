import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
import psycopg2
from psycopg2.extras import RealDictCursor

with DAG(
    'hw_11_a-maslennikov',
    default_args={
    'depends_on_past': False,
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': datetime.timedelta(minutes=5),
    },
    description = "Making DAG for 11th task",
    schedule_interval = datetime.timedelta(days=1),
    start_date = datetime.datetime(2023, 5, 26),
    catchup = False,
    tags = ["hw_11_a-maslennikov"],
) as dag:

    def get_the_most_active_user():
        postgres = PostgresHook(postgres_conn_id = "startml_feed")
        with postgres.get_conn() as conn:
            with conn.cursor(cursor_factory = RealDictCursor) as cursor:
                cursor.execute(
                    """
                    SELECT COUNT(action), user_id
                    FROM feed_action
                    WHERE action = 'like'
                    GROUP BY user_id
                    ORDER BY COUNT(action) DESC
                    LIMIT 1
                    """
                )
                result = cursor.fetchone()
                return result

    t1 = PythonOperator(
        task_id = "get_the_most_active_user",
        python_callable = get_the_most_active_user
    )

    t1