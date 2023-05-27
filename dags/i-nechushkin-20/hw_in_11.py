"""
Lesson KC Airflow
Task 11
"""
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python_operator import PythonOperator


def example_connection():
    from airflow.hooks.base import BaseHook
    import psycopg2
    from psycopg2.extras import RealDictCursor # error 2

    my_conn = BaseHook.get_connection(conn_id="startml_feed") # error 3 conn_id
    with psycopg2.connect(
            f"postgresql://{my_conn.login}:{my_conn.password}"
            f"@{my_conn.host}:{my_conn.port}/{my_conn.schema}"
    ) as conn:
        with conn.cursor(cursor_factory=RealDictCursor) as cursor:
            cursor.execute("""
            SELECT user_id, count(*)
            FROM feed_action
            WHERE action = 'like'
            GROUP BY user_id
            ORDER BY count(*) DESC
            LIMIT 1
            """)
            results = cursor.fetchone()  # error 1 indent
    return results


with DAG(
        'Task_11',
        # DAG dafault parameters
        default_args={
            'depends_on_past': False,
            'email': ['airflow@example.com'],
            'email_on_failure': False,
            'email_on_retry': False,
            'retries': 1,
            'retry_delay': timedelta(minutes=5),
        },
        start_date=datetime.now(),
        tags=['i-nechushkin-20_Task_11'],
) as dag:
    t = PythonOperator(
        task_id='example_conn',
        python_callable=example_connection,
    )

    t
