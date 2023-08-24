from datetime import datetime, timedelta
from textwrap import dedent

from airflow import DAG

from airflow.operators.bash import BashOperator
from airflow.operators.python_operator import PythonOperator


def get_connection():
    from airflow.providers.postgres.operators.postgres import PostgresHook
    postgres = PostgresHook(postgres_conn_id="startml_feed")

    query = """
    SELECT
    user_id,
    COUNT(*)
    FROM feed_action 
    WHERE action='like'
    GROUP BY user_id
    ORDER BY COUNT(*) DESC
    LIMIT 1
    """
    with postgres.get_conn() as conn:
        with conn.cursor() as cursor:
            cursor.execute(query)
            result = cursor.fetchone()
    return result


with DAG(
        'hw_n-shishkin_11',
        default_args={
            'depends_on_past': False,
            'email': ['airflow@example.com'],
            'email_on_failure': False,
            'email_on_retry': False,
            'retries': 1,
            'retry_delay': timedelta(minutes=5),  # timedelta из пакета datetime
        },
        schedule_interval=timedelta(days=1),
        start_date=datetime(2022, 1, 1),
        catchup=False,
        tags=['hw_n-shishkin_11'],
) as dag:
    t = PythonOperator(
        task_id="get_user",
        python_callable=get_connection
    )
