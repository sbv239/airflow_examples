from datetime import datetime, timedelta
from airflow import DAG
from psycopg2.extras import RealDictCursor
from airflow.operators.python import PythonOperator

def check_connection():
    from airflow.providers.postgres.operators.postgres import PostgresHook
    import psycopg2

    postgres = PostgresHook(postgres_conn_id='startml_feed', cursor_factory=RealDictCursor)
    with postgres.get_conn() as conn:
        cursor = conn.cursor()
        cursor.execute("""
            SELECT user_id, COUNT(*)
            FROM feed_action
            WHERE action = 'like'
            GROUP BY user_id
            ORDER BY COUNT(*) DESC
            LIMIT 1
        """)
        return cursor.fetchone()

with DAG(
    'hw_11_r-nazmiev',
    default_args={
        'depends_on_past': False,
        'email': ['airflow@example.com'],
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 1,
        'retry_delay': timedelta(minutes=5),
    },
    schedule_interval=timedelta(days=1),
    start_date=datetime(2023, 7, 1),
    catchup=False,
) as dag:
    task = PythonOperator(
        task_id='retrieve_top_liked_user',
        python_callable=check_connection,
    )
    task
