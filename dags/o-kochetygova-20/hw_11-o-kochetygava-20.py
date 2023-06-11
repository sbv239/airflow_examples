from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator

with DAG(
    'hw_11_o-kochetygova-20',
    default_args={
        'depends_on_past': False,
        'email': ['airflow@example.com'],
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 1,
        'retry_delay': timedelta(minutes=5),  # timedelta из пакета datetime
    },
    description='hw_11_o-kochetygova_DAG',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2022, 1, 1),
    catchup=False,
    tags=['task_11'],
) as dag:

    def get_activate_user():
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
                    SELECT f.user_id, COUNT (f.user.id)
                    FROM feed_action f
                    WHERE f.action = 'like'
                    GROUP BY f.user_id
                    ORDER BY COUNT (f.user_id) DESK
                    LIMIT 1 
                    """
                )
                results = cursor.fetchone()
            return results

        t3 = PythonOperator(
            task_id="postgres_querty",
            python_callable=get_activate_user,
        )