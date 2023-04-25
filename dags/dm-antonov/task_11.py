from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator


def find_top():
    from airflow.providers.postgres.hooks.postgres import PostgresHook
    from psycopg2.extras import RealDictCursor

    postgres = PostgresHook(postgres_conn_id='startml_feed')
    with postgres.get_conn() as conn:
        with conn.cursor(cursor_factory=RealDictCursor) as cursor:
            cursor.execute(
                """
                SELECT user_id, COUNT(action)
                FROM feed_action
                WHERE action = 'like'
                GROUP BY user_id
                ORDER BY COUNT(action) DESC
                LIMIT 1
                """
            )
            return cursor.fetchone()


with DAG(
        'task_11_dm-antonov',
        default_args={
            'depends_on_past': False,
            'email': ['airflow@example.com'],
            'email_on_failure': False,
            'email_on_retry': False,
            'retries': 1,
            'retry_delay': timedelta(minutes=5),
        },
        description='task_11',
        schedule_interval=timedelta(days=1),
        start_date=datetime(2023, 4, 23),
        catchup=False,
        tags=['task_11']
) as dag:
    t1 = PythonOperator(
        task_id='sql_feed',
        python_callable=find_top
    )
