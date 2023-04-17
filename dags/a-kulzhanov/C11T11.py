
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator


def get_id():
    from airflow.hooks.base import BaseHook
    import psycopg2

    creds = BaseHook.get_connection("startml_feed")
    with psycopg2.connect(
            f"postgresql://{creds.login}:{creds.password}"
            f"@{creds.host}:{creds.port}/{creds.schema}"
    ) as conn:
        with conn.cursor() as cursor:
            cursor.execute(
                """
            SELECT user_id, COUNT(post_id)
            FROM feed_action
            WHERE action = 'like'
            GROUP BY user_id
            ORDER BY COUNT(post_id) DESC 
            LIMIT 1
            """
                )
            results = cursor.fetchone()
            user = {"user_id": results[0], "count": results[1]}
        return user


with DAG(
        'aakulzhanov_task_11',
        default_args={
            'depends_on_past': False,
            'email': ['airflow@example.com'],
            'email_on_failure': False,
            'email_on_retry': False,
            'retries': 1,
            'retry_delay': timedelta(minutes=5),  # timedelta из пакета datetime
        },
        description='A simple Task 11',
        schedule_interval=timedelta(days=1),
        start_date=datetime(2023, 1, 1),
        catchup=False,
        tags=['example'],
) as dag:
    task_1 = PythonOperator(
      task_id="user_w_like",
      python_callable=get_id
    )
