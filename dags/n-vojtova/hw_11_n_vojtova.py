from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta

with DAG (
    "hw_11_n_vojtova",
    default_args={
        'depends_on_past': False,
        'email': ['airflow@example.com'],
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 1,
        'retry_delay': timedelta(minutes=5),  # timedelta из пакета datetime
    },
    description='DAG with connection',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2023,12,10),
    catchup=False,
    tags=['hw_11','n_vojtova'],
) as dag:
        def get_active_user():
                from airflow.hooks.base import BaseHook
                import psycopg2
                from psycopg2.extras import RealDictCursor

                creds = BaseHook.get_connection("startml_feed")
                with psycopg2.connect(
                    f"postgresql://{creds.login}:{creds.password}"
                    f"@{creds.host}:{creds.port}/{creds.schema}"
                ) as conn:
                    with conn.cursor(cursor_factory=RealDictCursor) as cursor:
                        cursor.execute(
                        """
                        
                        SELECT f.user_id, COUNT(f.user_id)
                        FROM feed_action f
                        WHERE f.action = 'like'
                        GROUP BY f.user_id
                        ORDER BY COUNT(f.user_id) DESC
                        LIMIT 1
                        """
                        )
                        results = cursor.fetchone()
                return results
        t1 = PythonOperator(
                task_id = 'postgres_connection',
                python_callable = get_active_user,
        )