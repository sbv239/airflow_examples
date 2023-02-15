from datetime import timedelta, datetime
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresHook


def get_data():
    postgres = PostgresHook(postgres_conn_id="startml_feed")
    with postgres.get_conn() as conn:   # вернет тот же connection, что вернул бы psycopg2.connect(...)
        with conn.cursor() as cursor:
            cursor.execute("""
            SELECT user_id, COUNT(post_id) as count
            FROM feed_action
            WHERE action = 'like'
            GROUP BY user_id
            ORDER BY count DESC
            LIMIT 1
            """)
            return cursor.fetchone()


with DAG(
        '11_dm-morozov',
        default_args={
            'depends_on_past': False,
            'email': ['airflow@example.com'],
            'email_on_failure': False,
            'email_on_retry': False,
            'retries': 1,
            'retry_delay': timedelta(minutes=5),  # timedelta из пакета datetime
        },
        start_date=datetime(2023, 2, 14)
) as dag:
    task = PythonOperator(
        task_id='simple_connection',
        python_callable=get_data
    )

    task
