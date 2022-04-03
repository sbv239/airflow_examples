from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from textwrap import dedent

default_args = {
        'depends_on_past': False,
        'email': ['airflow@example.com'],
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 1,
        'retry_delay': timedelta(minutes=5)
    }


def get_top_user():
    from airflow.providers.postgres.operators.postgres import PostgresHook

    postgres = PostgresHook(postgres_conn_id="startml_feed")
    with postgres.get_conn() as conn:
        with conn.cursor() as cursor:
            cursor.execute("""
            SELECT
                user_id,
                COUNT(*) AS amt
            FROM feed_action
            WHERE action = 'like'
            ORDER BY amt DESC
            LIMIT 1
            """)
            return cursor.fetchall()

with DAG(
    'hw_11_d.alenin-10',
    default_args=default_args,
    description='Simple first dag',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2022, 3, 20),
    catchup=False
) as dag:

    t = PythonOperator(
        task_id="get_top_user",
        python_callable=get_top_user
    )
