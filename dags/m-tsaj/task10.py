from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import psycopg2


def get_query():
    from airflow.providers.postgres.operators.postgres import PostgresHook

    postgres = PostgresHook(postgres_conn_id='startml_feed')

    with postgres.get_conn() as conn:
        with conn.cursor() as cursor:
            cursor.execute(
                """
               SELECT user_id, COUNT(action)
               FROM feed_action
               WHERE action = "like"
               GROUP BY user_id
               ORDER BY COUNT(action) DESC
               LIMIT 1
                """
            )
            return cursor.fetchone()


with DAG(
        'dag_10_m-tsaj',
        default_args={
            'depends_on_past': False,
            'email': ['airflow@example.com'],
            'email_on_failure': False,
            'email_on_retry': False,
            'retries': 1,
            'retry_delay': timedelta(minutes=5),
        },
        description='Simple connection dag',
        schedule_interval=timedelta(days=1),
        start_date=datetime(2021, 3, 20),
        catchup=False,
) as dag:
    t1 = PythonOperator(
        task_id='get_most_active_user',
        python_callable=get_query,
    )

    t1
