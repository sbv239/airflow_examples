from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from airflow.hooks.base import BaseHook
import psycopg2
from psycopg2.extras import RealDictCursor



def get_connection():
    creds = BaseHook.get_connection(conn_id='startml_feed')
    with psycopg2.connect(
            f"postgresql://{creds.login}:{creds.password}"
            f"@{creds.host}:{creds.port}/{creds.schema}",
            cursor_factory=RealDictCursor
    ) as conn:
        with conn.cursor() as cursor:
            cursor.execute("""                    
        SELECT user_id, COUNT(post_id)
        FROM feed_action
        WHERE action = 'like'
        GROUP BY user_id
        ORDER BY COUNT(post_id) DESC
        LIMIT 1;
       """)
            results = cursor.fetchone()
            return results


default_args = {
    'depends_on_past': False,
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
        'hw_11_d-korjakov',
        description='connection DAG',
        default_args=default_args,
        start_date=datetime(2023, 9, 24),
        schedule_interval=timedelta(days=1),
) as dag:
    t1 = PythonOperator(
        task_id='connection_to_SQL',
        python_callable=get_connection,
    )

    t1
