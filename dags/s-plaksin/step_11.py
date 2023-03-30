from datetime import timedelta, datetime
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresHook



def get_likes():
    postgres = PostgresHook(postgres_conn_id='startml_feed')
    with postgres.get_conn() as conn:  # вернет тот же connection, что вернул бы psycopg2.connect(...)
        with conn.cursor() as cursor:
            cursor.execute("""
            SELECT user_id, COUNT (*)
            FROM "feed_action"
            WHERE action = 'like'
            GROUP BY user_id
            ORDER BY COUNT(*) DESC
            LIMIT 1
            """)
            return cursor.fetchone()


with DAG(
        'hw_11_s-plaksin',
        default_args={
            'depends_on_past': False,
            'email': ['airflow@example.com'],
            'email_on_failure': False,
            'email_on_retry': False,
            'retries': 1,
            'retry_delay': timedelta(minutes=5),  # timedelta из пакета datetime
        },
        description='Connections',
        schedule_interval=timedelta(days=1),
        start_date=datetime(2023, 3, 28),
        catchup=False,
        tags=['hw_11'],
) as dag:
    task = PythonOperator(
        task_id='max_likes_user',
        python_callable=get_likes
    )
